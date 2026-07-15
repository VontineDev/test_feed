"""Tor 프록시 공용 헬퍼 — NEWNYM 회로 교체와 요청 지터.

기존에 data/krx_flow_sync.py와 data/youtube_narrative_sync.py에
각각 복사돼 있던 stem NEWNYM 시퀀스를 통합 (2026-07 리팩토링 Phase A).
호출자별 정책(가드 조건, 로그 레벨, 교체 후 대기)은 각 호출자에 남긴다.
"""

from __future__ import annotations

import logging
import os
import random

logger = logging.getLogger(__name__)

_DEFAULT_CONTROL_PORT = 9151


def jittered_delay(base: float) -> float:
    """Tor 사용 시 요청 간격에 랜덤 지터 부여 (일정한 패턴으로 탐지되는 것 방지).

    TOR_PROXY 미설정 시 base 그대로 반환 (기존 동작 유지).
    """
    if not os.environ.get("TOR_PROXY"):
        return base
    lo = max(base, 1.0)
    return random.uniform(lo, lo + 1.5)


def send_newnym(control_port: int) -> None:
    """Tor control port로 SIGNAL NEWNYM 전송. 실패 시 예외 전파.

    stem이 PROTOCOLINFO로 쿠키 파일 경로를 자동 탐색해 인증하므로 별도
    쿠키 경로 설정이 필요 없다. stem 미설치(ImportError)·컨트롤 포트
    다운·인증 실패를 호출자가 구분해 처리할 수 있도록 예외를 삼키지
    않는다 — best-effort 버전은 new_identity() 사용.
    """
    from stem import Signal
    from stem.control import Controller

    with Controller.from_port(port=control_port) as controller:
        controller.authenticate()
        controller.signal(Signal.NEWNYM)


def new_identity(control_port: int | None = None) -> bool:
    """새 출구 노드 요청 (best-effort). 성공 True, 모든 실패는 False.

    호출자가 예외 핸들러 안에서 복구 경로로 사용하므로 어떤 실패도
    예외를 던지지 않는다. control port 자체가 꺼져있는 경우(Tor Browser
    기본 설정 등) 조용히 스킵 — 필수 기능이 아님.
    """
    port = control_port if control_port is not None else int(
        os.environ.get("TOR_CONTROL_PORT", str(_DEFAULT_CONTROL_PORT))
    )
    try:
        send_newnym(port)
        return True
    except Exception as e:
        logger.debug("[tor] NEWNYM 실패: %s", e)
        return False
