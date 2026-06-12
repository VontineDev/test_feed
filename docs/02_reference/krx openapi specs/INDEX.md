# KRX Open API 명세서 전체 목차

> **Base URL**: `https://data-dbg.krx.co.kr/svc/apis`  
> **공통 Request Parameter**: `basDd` (string) — 기준일자 (형식: YYYYMMDD)  
> **공통 Response 구조**: `{"OutBlock_1": [ ... ]}`

---

## 📁 카테고리별 목록

### 1. 지수 (Index) — `/idx`

| # | 파일명 | API 제목 | Endpoint | 데이터 제공 시작 |
|---|--------|----------|----------|-----------------|
| 1 | [idx_01_krx_dd_trd.md](idx_01_krx_dd_trd.md) | KRX 시리즈 일별시세정보 | `/idx/krx_dd_trd` | 2010.01.04 |
| 2 | [idx_02_kospi_dd_trd.md](idx_02_kospi_dd_trd.md) | KOSPI 시리즈 일별시세정보 | `/idx/kospi_dd_trd` | 2010.01.04 |
| 3 | [idx_03_kosdaq_dd_trd.md](idx_03_kosdaq_dd_trd.md) | KOSDAQ 시리즈 일별시세정보 | `/idx/kosdaq_dd_trd` | 2010.01.04 |
| 4 | [idx_04_bon_dd_trd.md](idx_04_bon_dd_trd.md) | 채권지수 시세정보 | `/idx/bon_dd_trd` | 2010.01.04 |
| 5 | [idx_05_drvprod_dd_trd.md](idx_05_drvprod_dd_trd.md) | 파생상품지수 시세정보 | `/idx/drvprod_dd_trd` | 2010.01.04 |

---

### 2. 주식 (Stock) — `/sto`

| # | 파일명 | API 제목 | Endpoint | 데이터 제공 시작 |
|---|--------|----------|----------|-----------------|
| 6 | [sto_01_stk_bydd_trd.md](sto_01_stk_bydd_trd.md) | 유가증권 일별매매정보 | `/sto/stk_bydd_trd` | 2010.01.04 |
| 7 | [sto_02_ksq_bydd_trd.md](sto_02_ksq_bydd_trd.md) | 코스닥 일별매매정보 | `/sto/ksq_bydd_trd` | 2010.01.04 |
| 8 | [sto_03_knx_bydd_trd.md](sto_03_knx_bydd_trd.md) | 코넥스 일별매매정보 | `/sto/knx_bydd_trd` | 2013.07.01 |
| 9 | [sto_04_sw_bydd_trd.md](sto_04_sw_bydd_trd.md) | 신주인수권증권 일별매매정보 | `/sto/sw_bydd_trd` | 2010.01.04 |
| 10 | [sto_05_sr_bydd_trd.md](sto_05_sr_bydd_trd.md) | 신주인수권증서 일별매매정보 | `/sto/sr_bydd_trd` | 2010.02.12 |
| 11 | [sto_06_stk_isu_base_info.md](sto_06_stk_isu_base_info.md) | 유가증권 종목기본정보 | `/sto/stk_isu_base_info` | 2010.01.04 |
| 12 | [sto_07_ksq_isu_base_info.md](sto_07_ksq_isu_base_info.md) | 코스닥 종목기본정보 | `/sto/ksq_isu_base_info` | 2010.01.04 |
| 13 | [sto_08_knx_isu_base_info.md](sto_08_knx_isu_base_info.md) | 코넥스 종목기본정보 | `/sto/knx_isu_base_info` | 2013.07.01 |

---

### 3. ETP (ETF / ETN / ELW) — `/etp`

| # | 파일명 | API 제목 | Endpoint | 데이터 제공 시작 |
|---|--------|----------|----------|-----------------|
| 14 | [etp_01_etf_bydd_trd.md](etp_01_etf_bydd_trd.md) | ETF 일별매매정보 | `/etp/etf_bydd_trd` | 2010.01.04 |
| 15 | [etp_02_etn_bydd_trd.md](etp_02_etn_bydd_trd.md) | ETN 일별매매정보 | `/etp/etn_bydd_trd` | 2014.11.17 |
| 16 | [etp_03_elw_bydd_trd.md](etp_03_elw_bydd_trd.md) | ELW 일별매매정보 | `/etp/elw_bydd_trd` | 2010.01.04 |

---

### 4. 채권 (Bond) — `/bon`

| # | 파일명 | API 제목 | Endpoint | 데이터 제공 시작 |
|---|--------|----------|----------|-----------------|
| 17 | [bon_01_kts_bydd_trd.md](bon_01_kts_bydd_trd.md) | 국채전문유통시장 일별매매정보 | `/bon/kts_bydd_trd` | 2010.01.04 |
| 18 | [bon_02_bnd_bydd_trd.md](bon_02_bnd_bydd_trd.md) | 일반채권시장 일별매매정보 | `/bon/bnd_bydd_trd` | 2010.01.04 |
| 19 | [bon_03_smb_bydd_trd.md](bon_03_smb_bydd_trd.md) | 소액채권시장 일별매매정보 | `/bon/smb_bydd_trd` | 2010.01.04 |

---

### 5. 파생상품 (Derivatives) — `/drv`

| # | 파일명 | API 제목 | Endpoint | 데이터 제공 시작 |
|---|--------|----------|----------|-----------------|
| 20 | [drv_01_fut_bydd_trd.md](drv_01_fut_bydd_trd.md) | 선물 일별매매정보 (주식선물 外) | `/drv/fut_bydd_trd` | 2010.01.04 |
| 21 | [drv_02_eqsfu_stk_bydd_trd.md](drv_02_eqsfu_stk_bydd_trd.md) | 주식선물(유가증권) 일별매매정보 | `/drv/eqsfu_stk_bydd_trd` | 2010.01.04 |
| 22 | [drv_03_eqkfu_ksq_bydd_trd.md](drv_03_eqkfu_ksq_bydd_trd.md) | 주식선물(코스닥) 일별매매정보 | `/drv/eqkfu_ksq_bydd_trd` | 2015.08.03 |
| 23 | [drv_04_opt_bydd_trd.md](drv_04_opt_bydd_trd.md) | 옵션 일별매매정보 (주식옵션 外) | `/drv/opt_bydd_trd` | 2010.01.04 |
| 24 | [drv_05_eqsop_bydd_trd.md](drv_05_eqsop_bydd_trd.md) | 주식옵션(유가증권) 일별매매정보 | `/drv/eqsop_bydd_trd` | 2010.01.04 |
| 25 | [drv_06_eqkop_bydd_trd.md](drv_06_eqkop_bydd_trd.md) | 주식옵션(코스닥) 일별매매정보 | `/drv/eqkop_bydd_trd` | 2017.06.26 |

---

### 6. 일반상품 (General) — `/gen`

| # | 파일명 | API 제목 | Endpoint | 데이터 제공 시작 |
|---|--------|----------|----------|-----------------|
| 26 | [gen_01_oil_bydd_trd.md](gen_01_oil_bydd_trd.md) | 석유시장 일별매매정보 | `/gen/oil_bydd_trd` | 2012.03.30 |
| 27 | [gen_02_gold_bydd_trd.md](gen_02_gold_bydd_trd.md) | 금시장 일별매매정보 | `/gen/gold_bydd_trd` | 2014.03.24 |
| 28 | [gen_03_ets_bydd_trd.md](gen_03_ets_bydd_trd.md) | 배출권 시장 일별매매정보 | `/gen/ets_bydd_trd` | 2015.01.12 |

---

### 7. ESG — `/esg`

| # | 파일명 | API 제목 | Endpoint | 데이터 제공 시작 |
|---|--------|----------|----------|-----------------|
| 29 | [esg_01_esg_etp_info.md](esg_01_esg_etp_info.md) | ESG 증권상품 정보 | `/esg/esg_etp_info` | 2020.01.02 |
| 30 | [esg_02_sri_bond_info.md](esg_02_sri_bond_info.md) | 사회책임투자채권 정보 | `/esg/sri_bond_info` | 2019.01.01 |
| 31 | [esg_03_esg_index_info.md](esg_03_esg_index_info.md) | ESG 지수 정보 | `/esg/esg_index_info` | 2020.01.02 |

---

## 🔑 공통 필드 설명

| 필드명 | 설명 |
|--------|------|
| `BAS_DD` | 기준일자 (YYYYMMDD 형식) |
| `ISU_CD` | 종목코드 (표준코드 또는 단축코드) |
| `ISU_NM` | 종목명 |
| `TDD_CLSPRC` / `CLSPRC_IDX` | 종가 |
| `CMPPREVDD_PRC` / `CMPPREVDD_IDX` | 전일 대비 |
| `FLUC_RT` | 등락률 (%) |
| `TDD_OPNPRC` | 시가 |
| `TDD_HGPRC` | 고가 |
| `TDD_LWPRC` | 저가 |
| `ACC_TRDVOL` | 누적 거래량 |
| `ACC_TRDVAL` | 누적 거래대금 |
| `MKTCAP` | 시가총액 |
| `LIST_SHRS` | 상장주식수(좌수) |

---

*총 31개 API · 7개 카테고리 · 생성일: 2026-05-04*
