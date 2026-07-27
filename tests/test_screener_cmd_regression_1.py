"""
Regression: ISSUE-QA-001 — test_screener_cmd.py had asyncio.run(main()) at module
level with no __main__ guard, causing pytest import to fire real production Telegram
messages (DB connect + 499-ticker screener send) on every test run.
Found by /qa on 2026-04-18
Report: .gstack/qa-reports/qa-report-test_feed-2026-04-18.md
"""
import ast
import pathlib


def test_screener_cmd_has_main_guard():
    """Importing test_screener_cmd must not trigger asyncio.run at module level."""
    src = (pathlib.Path(__file__).parent / "test_screener_cmd.py").read_text(encoding="utf-8")
    tree = ast.parse(src)

    # The safe pattern: asyncio.run() must be inside an If whose test is
    # a comparison of __name__ == "__main__"
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            call = node.value
            # Top-level bare asyncio.run(...) is the bug
            if isinstance(call.func, ast.Attribute) and call.func.attr == "run":
                raise AssertionError(
                    "test_screener_cmd.py has asyncio.run() at module level without "
                    "__main__ guard — this fires production Telegram sends on every pytest run."
                )
