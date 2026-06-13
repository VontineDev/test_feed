"""메타랝스 → 메타랩스 corp_name 수정 (dart_extractions UPDATE)"""
import asyncio, sys
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()
from core.db import create_pool

OLD = "메타랝스"  # 메타랝스 (U+B79D = 랝)
NEW = "메타랩스"  # 메타랩스 (U+B7A9 = 랩)

async def main():
    pool = await create_pool()
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM dart_extractions WHERE corp_name = $1", OLD
        )
        print(f"dart_extractions '{OLD}' 건수: {count}")
        if count > 0:
            await conn.execute(
                "UPDATE dart_extractions SET corp_name = $1 WHERE corp_name = $2",
                NEW, OLD
            )
            print(f"UPDATE 완료: {count}건 '{OLD}' → '{NEW}'")

        # 확인
        count2 = await conn.fetchval(
            "SELECT COUNT(*) FROM dart_extractions WHERE corp_name = $1", NEW
        )
        print(f"dart_extractions '{NEW}' 건수 (업데이트 후): {count2}")

    await pool.close()

asyncio.run(main())
