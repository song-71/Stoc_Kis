#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
텔레그램 메시지 전송 모듈
사용법: from telegMsg import tmsg
       tmsg("메시지 내용")
"""
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path

# 텔레그램 설정
BOT_TOKEN = "8282014572:AAFVC5IiVNLKq5BwGVfuJjkYi3lXdr9M1XY"  # BotFather가 준 토큰
CHAT_ID = "5901426769"  # userinfobot이 알려준 숫자

# [260625] 날짜별 텔레그램 로그 단일 기록 지점.
#   - 서버는 UTC 이므로 반드시 한국시각(KST=UTC+9) 기준으로 날짜/시각을 기록한다.
#   - 모든 프로그램이 tmsg() 한 곳을 거치므로, 여기서 기록하면
#     프로덕션(ws_realtime_trading.py) 종료 후 다른 프로그램이 보내는 메시지도
#     같은 날짜별 텔레로그에 빠짐없이 남는다.
_KST = timezone(timedelta(hours=9))
_TELE_LOG_DIR = Path(__file__).resolve().parent / "out" / "logs"


def _append_tele_log(msg):
    """날짜별 텔레그램 로그(out/logs/{YYMMDD}_telegram.log)에 한 줄 기록.
    형식: 'YYYY-MM-DD HH:MM:SS | {메시지}' (프로덕션 기존 형식과 동일).
    기록 실패는 텔레그램 전송을 막지 않도록 조용히 무시한다."""
    try:
        now = datetime.now(_KST)
        path = _TELE_LOG_DIR / f"{now.strftime('%y%m%d')}_telegram.log"
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"{now.strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n")
    except Exception:
        pass


def tmsg(msg, mode=None):
    """
    텔레그램 메시지 전송
    
    Args:
        msg: 전송할 메시지
        mode: 출력 모드
            - None (기본): 터미널 출력 + 텔레그램 전송
            - '-t': 텔레그램만 전송 (터미널 출력 안함)
    
    Examples:
        tmsg("메시지")      # 터미널 + 텔레그램
        tmsg("메시지", "-t") # 텔레그램만
    """
    try:
        #now = datetime.now().strftime("[%H:%M:%S]")
        #msg_ = f"{now} {msg}"
        msg_ = f"{msg}"

        # [260625] 날짜별 텔레로그 기록을 네트워크 호출보다 먼저 수행.
        #   _notify_async 처럼 데몬 스레드에서 fire-and-forget 으로 호출되는 경우에도
        #   기록이 전송 시도보다 앞서 남도록 하여 누락 위험을 최소화한다.
        _append_tele_log(msg_)

        # 텔레그램 API 호출
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": msg_}
        
        # [260521] timeout 추가: 텔레그램 장애 시 어떤 호출처에서도 무한 블록되지 않도록.
        # (connect 3초 / read 3초)
        response = requests.post(url, json=payload, timeout=(3, 3))
        
        # 콘솔 출력 (mode가 '-t'가 아닐 때만)
        if mode != '-t':
            print(msg_)
        
        # 응답 확인
        if response.status_code == 200:
            return True
        else:
            # 에러 메시지는 항상 출력
            print(f"⚠️ 텔레그램 메시지 전송 실패: {response.status_code}")
            return False
    
    except Exception as e:
        # 에러 메시지는 항상 출력
        print(f"❌ 텔레그램 메시지 전송 오류: {e}")
        return False

# 테스트용 코드 (모듈로 import될 때는 실행 안 됨)
if __name__ == "__main__":
    # 테스트 메시지 전송
    tmsg("텔레그램 메시지 모듈 테스트 완료!")

    # 다른 파일에서 이 모듈을 import해서 메시지 전송하는 예시:
    #
    # from telegMsg import tmsg
    # tmsg("보낼 메시지 내용")
    #
    # 실제 사용 예:
    #   from telegMsg import tmsg
    #   tmsg("트레이딩 봇이 시작되었습니다!")
