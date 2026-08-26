"""core/db_schema.py에 정의된 모든 테이블이 RLS 활성화 목록에 등록돼 있는지 검증.

배경: dart_fundamentals 테이블이 CREATE TABLE 정의는 있었지만 _RLS_ALWAYS 목록에서
누락되어 Supabase 보안 어드바이저의 "RLS Disabled in Public" 경고가 발생했다
(2026-08-26). 이 테스트는 같은 종류의 누락을 코드 리뷰 없이도 즉시 잡아낸다.
"""
import re

from core import db_schema


def _table_names_from_ddl(ddl: str) -> list[str]:
    return re.findall(
        r"CREATE TABLE IF NOT EXISTS\s+(\w+)", ddl, flags=re.IGNORECASE
    )


def _all_defined_tables() -> set[str]:
    ddl_constants = [
        name for name in vars(db_schema) if name.startswith("_CREATE")
    ]
    tables: set[str] = set()
    for name in ddl_constants:
        value = getattr(db_schema, name)
        if isinstance(value, str):
            tables.update(_table_names_from_ddl(value))
    return tables


def test_every_defined_table_has_rls_entry():
    defined = _all_defined_tables()
    assert defined, "DDL 상수에서 테이블 이름을 하나도 찾지 못했다 — 정규식/구조 변경 확인 필요"

    rls_covered = set(db_schema._RLS_ALWAYS) | set(db_schema._RLS_IF_EXISTS)
    missing = defined - rls_covered

    assert not missing, (
        f"다음 테이블이 core/db_schema.py에 정의돼 있지만 "
        f"_RLS_ALWAYS / _RLS_IF_EXISTS 어디에도 없다: {sorted(missing)}. "
        "Supabase 어드바이저의 'RLS Disabled in Public' 경고를 유발한다."
    )
