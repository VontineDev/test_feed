"""자막 수집 디버그 — 결과를 파일로 저장."""
import os, sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from datetime import date, timedelta
from data.youtube_narrative_sync import fetch_video_list
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled

api_key = os.environ.get("YOUTUBE_API_KEY", "")
today   = date.today()
videos  = fetch_video_list(api_key, today - timedelta(days=7), today)

log = [f"영상 목록: {len(videos)}개\n"]
ok_videos = []

api = YouTubeTranscriptApi()
for v in videos[:20]:
    vid = v["video_id"]
    try:
        tlist = api.list(vid)
        langs = [t.language_code for t in tlist]
        fetched = api.fetch(vid, languages=["ko", "ko-KR"])
        text = " ".join(s.text for s in fetched)
        log.append(f"OK  {v['video_date']}  {v['title'][:50]}  | langs={langs} | {len(text)}자")
        ok_videos.append((v, text))
    except TranscriptsDisabled:
        log.append(f"DIS {v['video_date']}  {v['title'][:50]}")
    except NoTranscriptFound:
        log.append(f"NON {v['video_date']}  {v['title'][:50]}")
    except Exception as e:
        log.append(f"ERR {v['video_date']}  {vid}  {type(e).__name__}: {str(e)[:80]}")

log.append(f"\n자막 확보: {len(ok_videos)}개")
out = ROOT / "docs" / "_transcript_debug.txt"
out.write_text("\n".join(log), encoding="utf-8")
print("done")
