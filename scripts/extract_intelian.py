"""인텔리안테크 DART 보고서 추출 및 DB 저장"""
import asyncio, sys
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()
import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

from core.db import create_pool
from data.dart_extractor import extract_all

async def main():
    pool = await create_pool()
    n = await extract_all(pool, force=True, corp_filter="인텔리안테크")
    print(f"저장/갱신: {n}건")
    await pool.close()

asyncio.run(main())
