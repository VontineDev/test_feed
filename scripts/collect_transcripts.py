"""삼프로TV 자막 수집 → docs/삼프로TV_자막샘플.md 생성.

yt-dlp로 자막 URL 추출 → httpx로 직접 다운로드 (포맷 선택 에러 우회).
"""
import json
import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

COOKIES_FILE = ROOT / "docs" / "youtube.com_cookies.txt"


def _parse_json3(text: str) -> str:
    """YouTube json3 자막 → 텍스트."""
    data   = json.loads(text)
    events = data.get("events", [])
    parts  = []
    for ev in events:
        for seg in ev.get("segs", []):
            t = seg.get("utf8", "").replace("\n", " ").strip()
            if t:
                parts.append(t)
    return " ".join(parts).strip()


def fetch_transcript_direct(video_id: str) -> str | None:
    """yt-dlp로 자막 URL 추출 → httpx로 직접 다운로드."""
    import httpx
    import yt_dlp

    ydl_opts: dict[str, Any] = {
        "cookiefile": str(COOKIES_FILE),
        "quiet": True,
        "no_warnings": True,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:  # type: ignore[arg-type]
            info = ydl.extract_info(
                f"https://www.youtube.com/watch?v={video_id}",
                download=False,
                process=False,
            )
        auto = info.get("automatic_captions", {})
        subs = info.get("subtitles", {})

        # ko 자막 우선 (수동 > 자동)
        ko_entries = subs.get("ko") or subs.get("ko-KR") or \
                     auto.get("ko") or auto.get("ko-KR") or []
        if not ko_entries:
            return None

        # json3 URL 우선, 없으면 srv1
        url = None
        for fmt in ["json3", "srv1", "ttml"]:
            for entry in ko_entries:
                if entry.get("ext") == fmt:
                    url = entry.get("url")
                    break
            if url:
                break
        if not url and ko_entries:
            url = ko_entries[0].get("url")
        if not url:
            return None

        # 자막 직접 다운로드
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        r = httpx.get(url, headers=headers, timeout=15, follow_redirects=True)
        r.raise_for_status()

        # 형식 파싱
        content = r.text
        if url.endswith("json3") or "json3" in url:
            text = _parse_json3(content)
        else:
            # XML/srv 형식 — 태그 제거
            text = re.sub(r"<[^>]+>", " ", content)
            text = re.sub(r"\s+", " ", text).strip()

        return text if len(text) >= 200 else None

    except Exception as e:
        print(f"  ERR {video_id}: {e}")
        return None


if __name__ == "__main__":
    from dotenv import load_dotenv
    import httpx
    import yt_dlp
    from data.youtube_narrative_sync import fetch_video_list

    load_dotenv(ROOT / ".env")
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

    api_key = os.environ.get("YOUTUBE_API_KEY", "")
    COOKIES_FILE = ROOT / "docs" / "youtube.com_cookies.txt"
    MAX_VIDEOS = 10

    if not api_key:
        sys.exit("YOUTUBE_API_KEY 미설정")
    if not COOKIES_FILE.exists():
        sys.exit(f"쿠키 파일 없음: {COOKIES_FILE}")

    # ── 영상 목록 ─────────────────────────────────────────────
    today = date.today()
    videos = fetch_video_list(api_key, today - timedelta(days=14), today)
    print(f"영상 목록: {len(videos)}개 (최근 14일)\n")

    # ── 자막 수집 ─────────────────────────────────────────────
    collected: list[tuple[dict, str]] = []
    for v in videos:
        if len(collected) >= MAX_VIDEOS:
            break
        vid = v["video_id"]
        url = f"https://www.youtube.com/watch?v={vid}"
        title = v["title"]
        print(f"[{len(collected)+1:2d}/?]  {v['video_date']}  {title[:55]}")
        print(f"        {url}")

        text = fetch_transcript_direct(vid)
        if text:
            print(f"        -> 자막 {len(text):,}자 확보\n")
            collected.append((v, text))
        else:
            print(f"        -> 자막 없음\n")

    print(f"총 자막 확보: {len(collected)}개")

    # ── MD 생성 ───────────────────────────────────────────────
    lines = [
        "# 삼프로TV 자막 샘플",
        "",
        f"> 수집일: {today}  |  채널: [삼프로TV 3PROTV](https://www.youtube.com/@3protv)  |  영상 {len(collected)}개",
        "",
    ]

    for v, transcript in collected:
        vid = v["video_id"]
        url = f"https://www.youtube.com/watch?v={vid}"
        lines += [
            "---",
            "",
            f"## {v['video_date']} — {v['title']}",
            "",
            f"**링크:** {url}",
            "",
            f"**자막 길이:** {len(transcript):,}자",
            "",
            "### 자막 전문",
            "",
            "```",
            transcript,
            "```",
            "",
        ]

    out = ROOT / "docs" / "삼프로TV_자막샘플.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n저장 완료: {out}  ({out.stat().st_size:,} bytes)")
