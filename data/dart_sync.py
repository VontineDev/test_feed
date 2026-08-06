"""
dart_sync.py — OpenDART 전자공시 데이터 수집

레이어 역할:
  - dart_companies  : DART 기업 고유번호 마스터 (corpCode.zip 기반)
  - dart_disclosures: 일별 공시 이벤트 (실적발표·유상증자·최대주주변경 등)
  - dart_xbrl       : 사업보고서 XBRL 재무수치 (Top 20 기업 월별 갱신)

Finding 3 해결:
  공시 INSERT 전에 corp_code 존재 여부를 확인하고 없으면 ON CONFLICT DO NOTHING으로
  dart_companies에 자동 시드한다. 다음 월별 갱신 시 정식 데이터로 완성된다.

실행:
  python data/dart_sync.py --seed-companies        # corp 마스터 초기 시드
  python data/dart_sync.py --sync-disclosures       # 전일 공시 수집 (스케줄러용)
  python data/dart_sync.py --sync-xbrl 2024         # 특정 연도 XBRL 갱신
"""
from __future__ import annotations

import argparse
import asyncio
import io
import json
import logging
import os
import re
import sys
import zipfile
from datetime import date, timedelta
from typing import Optional
from xml.etree import ElementTree as ET

import httpx
import warnings
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)

_BASE_URL = "https://opendart.fss.or.kr/api"

# 코스피 시총 Top 20 KRX 종목코드 (6자리)
TOP20_STOCK_CODES = [
    "005930",  # 삼성전자
    "000660",  # SK하이닉스
    "207940",  # 삼성바이오로직스
    "005380",  # 현대차
    "068270",  # 셀트리온
    "005490",  # POSCO홀딩스
    "035420",  # NAVER
    "051910",  # LG화학
    "000270",  # 기아
    "028260",  # 삼성물산
    "012330",  # 현대모비스
    "066570",  # LG전자
    "003670",  # 포스코퓨처엠
    "032830",  # 삼성생명
    "055550",  # 신한지주
    "105560",  # KB금융
    "035720",  # 카카오
    "096770",  # SK이노베이션
    "017670",  # SK텔레콤
    "030200",  # KT
]

# 수집 대상 공시유형: 정기보고서(A) + 주요사항보고서(B)
_PBLNTF_TYPES = ["A", "B"]

# XBRL 보고서코드
_REPRT_ANNUAL = "11011"   # 사업보고서
_REPRT_HALF   = "11012"   # 반기보고서


# ── API 클라이언트 ────────────────────────────────────────────

class DartClient:
    def __init__(self, api_key: str):
        self._key = api_key
        self._http = httpx.AsyncClient(timeout=30.0)

    async def aclose(self):
        await self._http.aclose()

    async def fetch_corp_codes_zip(self) -> bytes:
        """DART 전체 기업 고유번호 목록 ZIP 다운로드."""
        resp = await self._http.get(
            f"{_BASE_URL}/corpCode.xml",
            params={"crtfc_key": self._key},
        )
        resp.raise_for_status()
        return resp.content

    async def fetch_disclosures(
        self,
        corp_code: str,
        bgn_de: str,
        end_de: str,
        pblntf_ty: str = "A",
    ) -> list[dict]:
        """단일 기업 공시 목록 조회. bgn_de/end_de: 'YYYYMMDD'."""
        resp = await self._http.get(
            f"{_BASE_URL}/list.json",
            params={
                "crtfc_key": self._key,
                "corp_code": corp_code,
                "bgn_de": bgn_de,
                "end_de": end_de,
                "pblntf_ty": pblntf_ty,
                "page_count": "100",
            },
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "000":
            return []
        return data.get("list", [])

    async def fetch_latest_annual_rcept(
        self,
        corp_code: str,
        bsns_year: str,
    ) -> Optional[str]:
        """기업의 특정 연도 사업보고서 접수번호(rcept_no) 조회. 없으면 None."""
        items = await self.fetch_disclosures(
            corp_code,
            bgn_de=f"{bsns_year}0101",
            end_de=f"{bsns_year}1231",
            pblntf_ty="A",
        )
        for item in items:
            if "사업보고서" in (item.get("report_nm") or ""):
                return item.get("rcept_no")
        return None

    async def fetch_document_zip(self, rcept_no: str) -> bytes:
        """사업보고서 원문 ZIP 다운로드 (document.xml 엔드포인트)."""
        resp = await self._http.get(
            f"{_BASE_URL}/document.xml",
            params={"crtfc_key": self._key, "rcept_no": rcept_no},
            timeout=90.0,
        )
        resp.raise_for_status()
        return resp.content

    async def fetch_xbrl(
        self,
        corp_code: str,
        bsns_year: str,
        reprt_code: str = _REPRT_ANNUAL,
        fs_div: str = "CFS",
    ) -> list[dict]:
        """XBRL 단일회사 전체 재무제표 조회."""
        resp = await self._http.get(
            f"{_BASE_URL}/fnlttSinglAcntAll.json",
            params={
                "crtfc_key": self._key,
                "corp_code": corp_code,
                "bsns_year": bsns_year,
                "reprt_code": reprt_code,
                "fs_div": fs_div,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "000":
            return []
        return data.get("list", [])


# ── corp 마스터 시드 ──────────────────────────────────────────

def _parse_corp_codes(zip_bytes: bytes) -> list[dict]:
    """corpCode.zip → [{"corp_code", "corp_name", "stock_code", "market"}, ...]"""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        xml_name = next(n for n in zf.namelist() if n.endswith(".xml"))
        xml_bytes = zf.read(xml_name)

    root = ET.fromstring(xml_bytes)
    records = []
    for item in root.findall("list"):
        corp_code  = (item.findtext("corp_code") or "").strip()
        corp_name  = (item.findtext("corp_name") or "").strip()
        stock_code = (item.findtext("stock_code") or "").strip() or None
        if corp_code:
            records.append({
                "corp_code":  corp_code,
                "corp_name":  corp_name,
                "stock_code": stock_code,
                "market":     None,   # ZIP에 시장 구분 없음 — 별도 매핑 불필요
            })
    return records


async def seed_corp_companies(pool, api_key: Optional[str] = None) -> int:
    """
    DART corpCode.zip을 다운받아 dart_companies를 전체 upsert.
    반환: upsert 건수.
    """
    key = api_key or os.environ.get("DART_API_KEY")
    if not key:
        raise RuntimeError("DART_API_KEY 환경변수가 설정되지 않았습니다.")

    client = DartClient(key)
    try:
        logger.info("[dart] corpCode.zip 다운로드 중...")
        zip_bytes = await client.fetch_corp_codes_zip()
        records = _parse_corp_codes(zip_bytes)
        logger.info("[dart] 기업 목록 파싱 완료: %d건", len(records))
    finally:
        await client.aclose()

    # 배치 upsert (1000건씩)
    BATCH = 1000
    count = 0
    async with pool.acquire() as conn:
        for i in range(0, len(records), BATCH):
            batch = records[i : i + BATCH]
            await conn.executemany(
                """
                INSERT INTO dart_companies (corp_code, corp_name, stock_code, updated_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (corp_code) DO UPDATE SET
                    corp_name  = EXCLUDED.corp_name,
                    stock_code = EXCLUDED.stock_code,
                    updated_at = NOW()
                """,
                [(r["corp_code"], r["corp_name"], r["stock_code"]) for r in batch],
            )
            count += len(batch)
            logger.info("[dart] dart_companies upsert 진행: %d/%d", count, len(records))

    logger.info("[dart] dart_companies upsert 완료: %d건", count)
    return count


# ── Finding 3: INSERT 전 corp_code 자동 시드 ─────────────────

async def _ensure_corp(conn, corp_code: str, corp_name: str) -> None:
    """dart_companies에 corp_code 없으면 최소 정보로 시드 (FK 위반 방지)."""
    await conn.execute(
        """
        INSERT INTO dart_companies (corp_code, corp_name, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (corp_code) DO NOTHING
        """,
        corp_code,
        corp_name,
    )


# ── 공시 이벤트 수집 ─────────────────────────────────────────

async def sync_disclosures(
    pool,
    corp_codes: list[str],
    bgn_de: Optional[str] = None,
    end_de: Optional[str] = None,
    api_key: Optional[str] = None,
) -> int:
    """
    corp_codes 기업 목록에 대해 bgn_de~end_de 기간 공시를 dart_disclosures에 저장.
    bgn_de/end_de 미지정 시 전일 하루치.
    반환: 신규 저장 건수.
    """
    key = api_key or os.environ.get("DART_API_KEY")
    if not key:
        logger.warning("[dart] DART_API_KEY 미설정 — sync_disclosures 건너뜀")
        return 0

    yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
    bgn_de = bgn_de or yesterday
    end_de = end_de or yesterday

    client = DartClient(key)
    total = 0
    try:
        for corp_code in corp_codes:
            for pblntf_ty in _PBLNTF_TYPES:
                try:
                    items = await client.fetch_disclosures(
                        corp_code, bgn_de, end_de, pblntf_ty
                    )
                except Exception as e:
                    logger.warning("[dart] 공시 조회 실패 (%s/%s): %s", corp_code, pblntf_ty, e)
                    continue

                if not items:
                    continue

                async with pool.acquire() as conn:
                    for item in items:
                        rcept_no  = item.get("rcept_no", "").strip()
                        corp_name = item.get("corp_name", "").strip()
                        if not rcept_no:
                            continue

                        # Finding 3: FK 보장
                        await _ensure_corp(conn, corp_code, corp_name)

                        rcept_dt_str = item.get("rcept_dt", "")
                        rcept_dt = None
                        if len(rcept_dt_str) == 8:
                            try:
                                rcept_dt = date(
                                    int(rcept_dt_str[:4]),
                                    int(rcept_dt_str[4:6]),
                                    int(rcept_dt_str[6:]),
                                )
                            except ValueError:
                                pass

                        result = await conn.execute(
                            """
                            INSERT INTO dart_disclosures
                                (rcept_no, corp_code, corp_name, report_nm,
                                 rcept_dt, pblntf_ty, rm)
                            VALUES ($1,$2,$3,$4,$5,$6,$7)
                            ON CONFLICT (rcept_no) DO NOTHING
                            """,
                            rcept_no,
                            corp_code,
                            corp_name or None,
                            item.get("report_nm") or None,
                            rcept_dt,
                            pblntf_ty,
                            item.get("rm") or None,
                        )
                        if result.endswith("1"):
                            total += 1

            await asyncio.sleep(0.1)  # API 호출 간격

    finally:
        await client.aclose()

    logger.info("[dart] 공시 신규 저장: %d건 (기간: %s ~ %s)", total, bgn_de, end_de)
    return total


# ── XBRL 재무수치 수집 ───────────────────────────────────────

async def sync_xbrl(
    pool,
    corp_codes: list[str],
    bsns_year: Optional[str] = None,
    api_key: Optional[str] = None,
) -> int:
    """
    corp_codes에 대해 bsns_year 사업보고서 XBRL 재무수치를 dart_xbrl에 저장.
    bsns_year 미지정 시 전년도.
    반환: upsert 건수.
    """
    key = api_key or os.environ.get("DART_API_KEY")
    if not key:
        logger.warning("[dart] DART_API_KEY 미설정 — sync_xbrl 건너뜀")
        return 0

    if not bsns_year:
        bsns_year = str(date.today().year - 1)

    client = DartClient(key)
    total = 0
    try:
        for corp_code in corp_codes:
            for fs_div in ("CFS", "OFS"):
                try:
                    items = await client.fetch_xbrl(
                        corp_code, bsns_year, _REPRT_ANNUAL, fs_div
                    )
                except Exception as e:
                    logger.warning("[dart] XBRL 조회 실패 (%s/%s): %s", corp_code, fs_div, e)
                    continue

                if not items:
                    continue

                async with pool.acquire() as conn:
                    for item in items:
                        account_nm = (item.get("account_nm") or "").strip()
                        if not account_nm:
                            continue

                        # thstrm_amount: 당기 수치
                        raw_amt = item.get("thstrm_amount", "")
                        amount = None
                        if raw_amt:
                            try:
                                amount = int(str(raw_amt).replace(",", "").strip())
                            except ValueError:
                                pass

                        result = await conn.execute(
                            """
                            INSERT INTO dart_xbrl
                                (corp_code, bsns_year, reprt_code, account_nm,
                                 fs_div, amount, currency)
                            VALUES ($1,$2,$3,$4,$5,$6,'KRW')
                            ON CONFLICT (corp_code, bsns_year, reprt_code, account_nm, fs_div)
                            DO UPDATE SET
                                amount     = EXCLUDED.amount,
                                fetched_at = NOW()
                            """,
                            corp_code,
                            bsns_year,
                            _REPRT_ANNUAL,
                            account_nm,
                            fs_div,
                            amount,
                        )
                        if result.endswith("1"):
                            total += 1

            await asyncio.sleep(0.2)  # API 호출 간격

    finally:
        await client.aclose()

    logger.info("[dart] XBRL upsert 완료: %d건 (연도: %s)", total, bsns_year)
    return total


# ── 전체시장 재무 스냅샷 (dart_fundamentals) ────────────────────
# 2026-08-06: TechnicalQuant.md 펀더멘털 스크리닝(PBR/PER/ROE/부채비율/매출증가율)용.
# fnlttSinglAcntAll은 재무상태표(BS)+손익계산서(IS/CIS)+자본변동표(SCE) 등을 한
# 응답에 전부 섞어서 반환한다 — 같은 account_nm("자본총계"/"당기순이익" 등)이
# 여러 섹션에 중복 등장하므로 sj_div로 먼저 걸러야 정확한 합계를 얻는다.
# 당기순이익은 총계/지배기업귀속/비지배지분 3종이 같은 이름으로 나올 수 있어
# "먼저 나오는 행"(원본 재무제표 표시 순서상 총계가 최상단)을 채택한다.

_FUND_BS_TARGETS = {"자산총계": "assets", "부채총계": "liabilities", "자본총계": "equity"}
# 2026-08-06 라이브 확인: 회사마다 계정명 표기가 다르다 — "당기순이익" 대신
# "당기순이익(손실)"(적자 가능 표기), "매출액" 대신 "수익(매출액)"(IFRS 수익 인식
# 문구)을 쓰는 회사가 상당수라 별칭을 추가하지 않으면 매출/순이익 커버리지가
# 크게 떨어진다(최초 백필 시 revenue 47%, net_income 17%까지 하락 확인).
_FUND_IS_TARGETS = {
    "매출액": "revenue",
    "수익(매출액)": "revenue",
    "당기순이익": "net_income",
    "당기순이익(손실)": "net_income",
}


def _parse_amount(raw: Optional[str]) -> Optional[int]:
    if not raw:
        return None
    try:
        return int(str(raw).replace(",", "").strip())
    except ValueError:
        return None


def extract_fundamentals(items: list[dict]) -> dict[str, Optional[int]]:
    """fetch_xbrl() 응답에서 핵심 5개 계정(당기+전기)만 추출.

    반환 키: assets, liabilities, equity, revenue, revenue_prev, net_income
    (각 값 없으면 None). BS 섹션은 자산총계/부채총계/자본총계, IS/CIS 섹션은
    매출액/당기순이익만 본다 — sj_div 불일치 항목은 무시.
    """
    result: dict[str, Optional[int]] = {}
    for it in items:
        nm = (it.get("account_nm") or "").strip()
        sj = (it.get("sj_div") or "").strip()

        key = None
        if sj == "BS" and nm in _FUND_BS_TARGETS:
            key = _FUND_BS_TARGETS[nm]
        elif sj in ("IS", "CIS") and nm in _FUND_IS_TARGETS:
            key = _FUND_IS_TARGETS[nm]
        if key is None or key in result:
            continue  # 이미 채택됐으면 이후 중복(지배/비지배 분할 등)은 무시

        result[key] = _parse_amount(it.get("thstrm_amount"))
        if key == "revenue":
            result["revenue_prev"] = _parse_amount(it.get("frmtrm_amount"))
    return result


async def sync_fundamentals(
    pool,
    corp_stock_pairs: list[tuple[str, str]],
    bsns_year: Optional[str] = None,
    api_key: Optional[str] = None,
) -> int:
    """corp_stock_pairs=[(corp_code, stock_code), ...]에 대해 사업보고서 핵심
    재무수치를 dart_fundamentals에 upsert. CFS(연결) 우선, 없으면 OFS(별도) 폴백.
    반환: upsert 건수.
    """
    key = api_key or os.environ.get("DART_API_KEY")
    if not key:
        logger.warning("[dart] DART_API_KEY 미설정 — sync_fundamentals 건너뜀")
        return 0

    if not bsns_year:
        bsns_year = str(date.today().year - 1)

    client = DartClient(key)
    total = 0
    try:
        for corp_code, stock_code in corp_stock_pairs:
            fund = None
            used_fs_div = None
            for fs_div in ("CFS", "OFS"):
                try:
                    items = await client.fetch_xbrl(corp_code, bsns_year, _REPRT_ANNUAL, fs_div)
                except Exception as e:
                    logger.debug("[dart] fundamentals 조회 실패 (%s/%s): %s", corp_code, fs_div, e)
                    continue
                if not items:
                    continue
                extracted = extract_fundamentals(items)
                if extracted.get("revenue") is not None or extracted.get("equity") is not None:
                    fund, used_fs_div = extracted, fs_div
                    break  # CFS에서 값을 얻었으면 OFS는 볼 필요 없음

            if fund is None:
                await asyncio.sleep(0.2)
                continue

            async with pool.acquire() as conn:
                result = await conn.execute(
                    """
                    INSERT INTO dart_fundamentals
                        (corp_code, stock_code, bsns_year, fs_div,
                         revenue, revenue_prev, net_income, equity, liabilities, assets)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                    ON CONFLICT (corp_code, bsns_year) DO UPDATE SET
                        stock_code   = EXCLUDED.stock_code,
                        fs_div       = EXCLUDED.fs_div,
                        revenue      = EXCLUDED.revenue,
                        revenue_prev = EXCLUDED.revenue_prev,
                        net_income   = EXCLUDED.net_income,
                        equity       = EXCLUDED.equity,
                        liabilities  = EXCLUDED.liabilities,
                        assets       = EXCLUDED.assets,
                        fetched_at   = NOW()
                    """,
                    corp_code, stock_code, bsns_year, used_fs_div,
                    fund.get("revenue"), fund.get("revenue_prev"), fund.get("net_income"),
                    fund.get("equity"), fund.get("liabilities"), fund.get("assets"),
                )
                if result.endswith("1") or "UPDATE" in result:
                    total += 1

            await asyncio.sleep(0.2)  # API 호출 간격

    finally:
        await client.aclose()

    logger.info("[dart] fundamentals upsert 완료: %d건 (연도: %s)", total, bsns_year)
    return total


# ── 사업보고서 섹션 설정 ─────────────────────────────────────

_SECTION_TARGETS: dict[str, dict] = {
    "II-2": {
        "keywords": [
            "2. 주요 제품 및 서비스",
            "주요 제품 및 서비스",
            "2.주요 제품 및 서비스",
            "2. 주요제품 및 서비스",
            "주요제품 및 서비스",
        ],
        "stop_keywords": [
            "3. 원재료", "3.원재료", "원재료 및 생산", "3. 생산", "4. 매출",
        ],
        "prompt": (
            "아래는 한국 상장기업 사업보고서의 '주요 제품 및 서비스' 절입니다.\n"
            "제품/서비스 세그먼트 정보를 다음 JSON 형식으로 추출하세요:\n"
            '{{"products": [{{"name": "제품명", "description": "설명 1-2문장", '
            '"revenue_share_pct": 비율숫자또는null}}]}}\n'
            "없는 항목은 null. 순수 JSON만 출력, 추가 설명 없이:\n\n"
        ),
    },
    "II-4": {
        "keywords": [
            "4. 매출 및 수주상황",
            "매출 및 수주상황",
            "4.매출 및 수주",
            "4. 매출현황",
            "매출현황",
        ],
        "stop_keywords": [
            "5. 위험관리", "5.위험관리", "5. 파생", "6. ", "III.",
        ],
        "prompt": (
            "아래는 한국 상장기업 사업보고서의 '매출 및 수주상황' 절입니다.\n"
            "매출 세그먼트 정보를 다음 JSON 형식으로 추출하세요:\n"
            '{{"segments": [{{"name": "구분", "amount_billion_krw": 숫자또는null, '
            '"share_pct": 비율또는null, "yoy_pct": 전년비또는null}}], '
            '"total_billion_krw": 숫자또는null}}\n'
            "없는 항목은 null. 순수 JSON만 출력, 추가 설명 없이:\n\n"
        ),
    },
}

_MAX_SECTION_CHARS = 6000   # Ollama 입력 텍스트 최대 길이


# ── HTML → 텍스트 변환 헬퍼 ──────────────────────────────────

def _decode_html(raw: bytes) -> str:
    """바이트 → 문자열. UTF-8/CP949 순차 시도."""
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return raw.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    return raw.decode("utf-8", errors="replace")


def _soup_to_text(soup: "BeautifulSoup") -> str:
    """
    BS4 tree → 읽기 쉬운 평문.
    테이블은 파이프 구분 행으로, 나머지는 공백 정리 후 반환.
    """
    for script in soup(["script", "style"]):
        script.decompose()

    # 테이블 → 파이프 텍스트로 교체
    for table in soup.find_all("table"):
        rows_txt: list[str] = []
        for tr in table.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            cells = [c for c in cells if c]
            if cells:
                rows_txt.append(" | ".join(cells))
        span = soup.new_tag("span")
        span.string = "\n" + "\n".join(rows_txt) + "\n"
        table.replace_with(span)

    text = soup.get_text("\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def _extract_section_text(zip_bytes: bytes, section_id: str) -> str:
    """
    사업보고서 ZIP에서 대상 절(section_id) 텍스트 추출.
    ZIP 내 HTML 파일을 순서대로 검색해 첫 번째 히트를 반환.
    없으면 빈 문자열.
    """
    cfg = _SECTION_TARGETS[section_id]
    keywords: list[str] = cfg["keywords"]
    stop_kws: list[str] = cfg["stop_keywords"]

    try:
        zf_obj = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except zipfile.BadZipFile:
        logger.warning("[dart] 문서 ZIP 파싱 실패 — BadZipFile")
        return ""

    with zf_obj as zf:
        # DART4 XML 포맷 (.xml) + 구형 HTML 포맷 (.html/.htm) 모두 처리
        html_names = sorted(
            n for n in zf.namelist()
            if n.lower().endswith((".html", ".htm", ".xml"))
        )

        for name in html_names:
            try:
                raw = zf.read(name)
                html_str = _decode_html(raw)
                soup = BeautifulSoup(html_str, "html.parser")
                full_text = _soup_to_text(soup)
            except Exception as e:
                logger.debug("[dart] HTML 파싱 실패 (%s): %s", name, e)
                continue

            # 섹션 키워드 전체 발생 위치 수집
            all_starts: list[int] = []
            for kw in keywords:
                pos = 0
                while True:
                    found = full_text.find(kw, pos)
                    if found == -1:
                        break
                    all_starts.append(found)
                    pos = found + 1
            all_starts.sort()

            if not all_starts:
                continue

            # 각 발생 위치를 시도 — 줄 시작이 아닌 인라인 참조는 건너뜀, TOC(400자 미만)도 건너뜀
            for start in all_starts:
                # 인라인 참조 제외: 키워드가 줄 시작에 있지 않으면 건너뜀
                if start > 0 and full_text[start - 1] not in ("\n", "\r"):
                    continue
                tail = full_text[start:]
                end = len(tail)
                for sk in stop_kws:
                    idx = tail.find(sk, 30)   # 30: 섹션 헤더 자체 skip (200은 너무 커서 TOC 내 다음 항목 누락)
                    if 0 < idx < end:
                        end = idx

                result = tail[:end].strip()
                if len(result) < 400:
                    continue  # 목차 항목 — 건너뜀

                logger.debug(
                    "[dart] %s 섹션 추출 성공 (%s, %d자)",
                    section_id, name, len(result),
                )
                return result[:_MAX_SECTION_CHARS]

    return ""


# ── Ollama 구조화 파싱 ───────────────────────────────────────

def _extract_json(raw: str) -> Optional[dict]:
    """LLM 응답에서 JSON 객체 추출. 마크다운 코드블록 처리."""
    # 코드블록 제거
    text = re.sub(r"```(?:json)?\s*", "", raw).strip().rstrip("`").strip()
    # 추론 블록 제거 (<think>...</think>)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()

    # { ... } 형태의 첫 번째 JSON 객체 추출
    brace_start = text.find("{")
    if brace_start == -1:
        return None
    depth = 0
    for i, ch in enumerate(text[brace_start:], brace_start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[brace_start : i + 1])
                except json.JSONDecodeError:
                    return None
    return None


async def _call_ollama_segment_parse(
    section_text: str,
    section_id: str,
    corp_name: str,
) -> tuple[Optional[dict], str]:
    """
    Ollama로 섹션 텍스트 파싱.
    반환: (parsed_dict_or_None, model_name)
    """
    from reports.summarizer import (
        OLLAMA_MODEL,
        _call_ollama_native, _ollama_is_alive,
    )

    prompt = (
        f"기업명: {corp_name}\n\n"
        + _SECTION_TARGETS[section_id]["prompt"]
        + section_text
    )

    async with httpx.AsyncClient() as http:
        if not await _ollama_is_alive(http):
            logger.warning("[dart] Ollama 미실행 — 섹션 파싱 건너뜀")
            return None, OLLAMA_MODEL

        try:
            raw = await _call_ollama_native(
                http,
                OLLAMA_MODEL,
                prompt,
                timeout=120.0,
                max_tokens=800,
                system_prompt=(
                    "당신은 한국 기업 재무 문서 파싱 전문가입니다. "
                    "요청받은 JSON 스키마에 맞게 정보를 추출합니다. "
                    "반드시 유효한 JSON 객체만 반환하고, 추가 설명을 출력하지 마세요."
                ),
                enable_thinking=False,
            )
            parsed = _extract_json(raw)
            return parsed, OLLAMA_MODEL
        except Exception as e:
            logger.warning("[dart] Ollama 파싱 실패 (%s/%s): %s", corp_name, section_id, e)
            return None, OLLAMA_MODEL


# ── 세그먼트 내러티브 수집 ────────────────────────────────────

async def sync_segments(
    pool,
    corp_codes: list[str],
    bsns_year: Optional[str] = None,
    api_key: Optional[str] = None,
    force: bool = False,
) -> int:
    """
    corp_codes에 대해 bsns_year 사업보고서 II-2/II-4 절을 Ollama로 파싱 → dart_segments 저장.
    bsns_year 미지정 시 전년도.
    force=True 이면 이미 존재하는 레코드도 재파싱.
    반환: 저장/갱신 건수.
    """
    key = api_key or os.environ.get("DART_API_KEY")
    if not key:
        logger.warning("[dart] DART_API_KEY 미설정 — sync_segments 건너뜀")
        return 0

    if not bsns_year:
        bsns_year = str(date.today().year - 1)

    # corp_code → corp_name 매핑
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT corp_code, corp_name FROM dart_companies WHERE corp_code = ANY($1)",
            corp_codes,
        )
    name_map = {r["corp_code"]: r["corp_name"] for r in rows}

    client = DartClient(key)
    total = 0

    try:
        for corp_code in corp_codes:
            corp_name = name_map.get(corp_code, corp_code)
            logger.info("[dart] 세그먼트 파싱 시작: %s (%s)", corp_name, bsns_year)

            # 이미 처리됐는지 확인 (force=False 시 건너뜀)
            if not force:
                async with pool.acquire() as conn:
                    done = await conn.fetchval(
                        """
                        SELECT COUNT(*) FROM dart_segments
                        WHERE corp_code=$1 AND bsns_year=$2 AND parse_ok=TRUE
                        """,
                        corp_code, bsns_year,
                    )
                if done and done >= 2:
                    logger.debug("[dart] %s %s 이미 파싱됨 — 건너뜀", corp_name, bsns_year)
                    continue

            # 사업보고서 rcept_no 조회
            try:
                rcept_no = await client.fetch_latest_annual_rcept(corp_code, bsns_year)
            except Exception as e:
                logger.warning("[dart] rcept_no 조회 실패 (%s): %s", corp_name, e)
                continue

            if not rcept_no:
                logger.info("[dart] %s %s 사업보고서 없음", corp_name, bsns_year)
                continue

            # 문서 ZIP 다운로드
            try:
                logger.info("[dart] 문서 ZIP 다운로드: %s (rcept=%s)", corp_name, rcept_no)
                zip_bytes = await client.fetch_document_zip(rcept_no)
            except Exception as e:
                logger.warning("[dart] 문서 ZIP 실패 (%s): %s", corp_name, e)
                continue

            # 섹션별 추출 + Ollama 파싱
            for section_id in ("II-2", "II-4"):
                raw_text = _extract_section_text(zip_bytes, section_id)
                if not raw_text:
                    logger.info("[dart] %s %s 절 텍스트 없음", corp_name, section_id)
                    # 빈 레코드라도 저장 (재시도 불필요 표시)
                    async with pool.acquire() as conn:
                        await conn.execute(
                            """
                            INSERT INTO dart_segments
                                (corp_code, bsns_year, section, raw_text, parse_ok, fetched_at)
                            VALUES ($1,$2,$3,NULL,FALSE,NOW())
                            ON CONFLICT (corp_code, bsns_year, section) DO NOTHING
                            """,
                            corp_code, bsns_year, section_id,
                        )
                    continue

                parsed, model = await _call_ollama_segment_parse(
                    raw_text, section_id, corp_name
                )
                parse_ok = parsed is not None
                parsed_json_str = json.dumps(parsed, ensure_ascii=False) if parsed else None

                async with pool.acquire() as conn:
                    result = await conn.execute(
                        """
                        INSERT INTO dart_segments
                            (corp_code, bsns_year, section, raw_text,
                             parsed_json, parse_ok, model, fetched_at)
                        VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,NOW())
                        ON CONFLICT (corp_code, bsns_year, section) DO UPDATE SET
                            raw_text    = EXCLUDED.raw_text,
                            parsed_json = EXCLUDED.parsed_json,
                            parse_ok    = EXCLUDED.parse_ok,
                            model       = EXCLUDED.model,
                            fetched_at  = NOW()
                        """,
                        corp_code, bsns_year, section_id,
                        raw_text, parsed_json_str, parse_ok, model,
                    )
                if result.endswith("1"):
                    total += 1

                status = "OK" if parse_ok else "FAIL"
                logger.info(
                    "[dart] %s %s %s — %s (원본 %d자)",
                    corp_name, bsns_year, section_id, status, len(raw_text),
                )

            await asyncio.sleep(1.0)   # 문서 다운로드 간격

    finally:
        await client.aclose()

    logger.info("[dart] sync_segments 완료: %d건 저장/갱신", total)
    return total


# ── Top 20 corp_code 조회 헬퍼 ───────────────────────────────

async def get_top20_corp_codes(pool) -> list[str]:
    """dart_companies에서 TOP20_STOCK_CODES에 해당하는 corp_code 목록 반환."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT corp_code FROM dart_companies
            WHERE stock_code = ANY($1)
            ORDER BY corp_code
            """,
            TOP20_STOCK_CODES,
        )
    return [r["corp_code"] for r in rows]


# ── CLI 진입점 ────────────────────────────────────────────────

async def _main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="OpenDART 데이터 수집")
    parser.add_argument("--seed-companies", action="store_true",
                        help="corp 마스터 초기 시드 (corpCode.zip 전체 다운로드)")
    parser.add_argument("--sync-disclosures", action="store_true",
                        help="공시 이벤트 수집 (전일 기본)")
    parser.add_argument("--bgn-de", help="공시 시작일 YYYYMMDD")
    parser.add_argument("--end-de", help="공시 종료일 YYYYMMDD")
    parser.add_argument("--sync-xbrl", metavar="YEAR",
                        help="XBRL 재무수치 수집 (연도 미지정 시 전년도)")
    parser.add_argument("--sync-segments", metavar="YEAR",
                        help="사업보고서 II-2/II-4 Ollama 파싱 (연도 미지정 시 전년도)")
    parser.add_argument("--force", action="store_true",
                        help="이미 파싱된 레코드도 재처리 (--sync-segments 전용)")
    parser.add_argument("--corp-code", metavar="CODE",
                        help="단일 기업 대상 지정 (6자리 종목코드 또는 8자리 DART 코드)")
    args = parser.parse_args()

    from core.db import create_pool, init_db
    pool = await create_pool()
    await init_db(pool)

    # 단일 기업 지정 시 corp_code 해석
    async def _resolve_corp_codes() -> list[str]:
        if args.corp_code:
            code = args.corp_code.strip()
            if len(code) == 6:
                # KRX 종목코드 → DART corp_code 변환
                async with pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT corp_code FROM dart_companies WHERE stock_code=$1", code
                    )
                if not row:
                    print(f"stock_code={code}에 해당하는 dart_companies 레코드 없음. --seed-companies 먼저 실행 필요.")
                    return []
                return [row["corp_code"]]
            return [code]  # 8자리 DART 코드 직접 사용
        return await get_top20_corp_codes(pool)

    if args.seed_companies:
        n = await seed_corp_companies(pool)
        print(f"dart_companies 시드 완료: {n}건")

    elif args.sync_disclosures:
        corp_codes = await _resolve_corp_codes()
        if not corp_codes:
            print("대상 기업 없음.")
            return
        n = await sync_disclosures(pool, corp_codes, args.bgn_de, args.end_de)
        print(f"공시 수집 완료: {n}건")

    elif args.sync_xbrl is not None:
        corp_codes = await _resolve_corp_codes()
        if not corp_codes:
            print("대상 기업 없음.")
            return
        n = await sync_xbrl(pool, corp_codes, args.sync_xbrl or None)
        print(f"XBRL 수집 완료: {n}건")

    elif args.sync_segments is not None:
        corp_codes = await _resolve_corp_codes()
        if not corp_codes:
            print("dart_companies에 Top 20 종목 없음. 먼저 --seed-companies 실행 필요.")
            return
        n = await sync_segments(
            pool, corp_codes, args.sync_segments or None, force=args.force
        )
        print(f"세그먼트 파싱 완료: {n}건")

    else:
        parser.print_help()

    await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
