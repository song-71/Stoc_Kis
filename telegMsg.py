#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
텔레그램 메시지 전송 모듈
사용법: from telegMsg import tmsg
       tmsg("메시지 내용")
"""
import requests
from datetime import datetime

# 텔레그램 설정
BOT_TOKEN = "8282014572:AAFVC5IiVNLKq5BwGVfuJjkYi3lXdr9M1XY"  # BotFather가 준 토큰
CHAT_ID = "5901426769"  # userinfobot이 알려준 숫자

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
        
        # 텔레그램 API 호출
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": msg_}
        
        response = requests.post(url, json=payload)
        
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
