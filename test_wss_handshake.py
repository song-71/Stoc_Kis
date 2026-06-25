#!/usr/bin/env python3
"""KIS WSS 핸드셰이크 probe — 순수 연결(WebSocket 업그레이드)만 검사.

목적
----
KIS 실시간 WSS(`ws://ops.koreainvestment.com:21000`)에 대해 **TCP 도달성 + WebSocket
핸드셰이크(HTTP 101 업그레이드)** 만 검사한다. 구독(subscribe)도, approval_key 발급도
하지 않으므로 운영 중인 캐시 키를 무효화하지 않는다. (approval_key 는 핸드셰이크가 아니라
구독 메시지 본문에 실리므로, 핸드셰이크 성패는 approval_key 와 무관하다.)

쓰임새
------
- "연결이 안 된다 / InvalidMessage / dwell<15s 즉사" 류 증상이 **연결(IP/세션/off-hours)
  레벨인지, 아니면 그 위(구독/approval/데이터)** 인지 1차로 가른다.
- 장 시간대 vs 장 마감 후를 비교해 off-hours 거부 여부를 판별한다.
  · 장중(예: 09:00~15:30)에 OK → 핸드셰이크 정상. 문제는 그 위 계층.
  · 장중에도 FAIL(서버 RST) → 연결레벨 차단/throttle 의심.
  · 장 마감 후 FAIL 은 off-hours 가능성이 있어 단독으론 결론 못 냄.

사용법
------
    ./venv/bin/python test_wss_handshake.py            # 기본 5회
    ./venv/bin/python test_wss_handshake.py 10         # 10회
    ./venv/bin/python test_wss_handshake.py 10 0.5     # 10회, 간격 0.5초

종료코드: 0 = 1회 이상 핸드셰이크 성공, 1 = 전부 실패.
"""
from __future__ import annotations

import asyncio
import socket
import sys
import time
from datetime import datetime, timezone, timedelta

import websockets

KST = timezone(timedelta(hours=9))


def _ws_url() -> str:
    """SDK 와 동일한 WSS base URL 을 config 에서 그대로 읽는다(불일치 방지).
    실패 시 운영 기본값으로 폴백."""
    try:
        import kis_auth_llm as ka  # noqa: E402
        url = ka._cfg.get("ops")
        if url:
            return url
    except Exception as e:
        print(f"  (config 로드 실패 → 기본 URL 사용: {type(e).__name__}: {e})")
    return "ws://ops.koreainvestment.com:21000"


def _host_port(url: str) -> tuple[str, int]:
    body = url.split("://", 1)[-1]
    hostport = body.split("/", 1)[0]
    if ":" in hostport:
        h, p = hostport.rsplit(":", 1)
        return h, int(p)
    return hostport, (80 if url.startswith("ws://") else 443)


def tcp_probe(host: str, port: int, timeout: float = 5.0) -> tuple[bool, str]:
    """TCP 연결 + WS 업그레이드 요청 후 서버 raw 응답을 본다.
    서버가 HTTP 응답 없이 끊으면(RST) ConnectionReset → 연결레벨 거부 신호."""
    t0 = time.time()
    try:
        s = socket.create_connection((host, port), timeout=timeout)
    except Exception as e:
        return False, f"TCP 연결 실패 {type(e).__name__}: {e}"
    try:
        req = (
            "GET / HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        )
        s.sendall(req.encode())
        s.settimeout(timeout)
        resp = s.recv(4096)
        dwell = time.time() - t0
        if not resp:
            return False, f"TCP OK({dwell:.2f}s) → 서버가 빈 응답으로 종료 (업그레이드 거부)"
        head = resp.split(b"\r\n", 1)[0].decode("ascii", "replace")
        return True, f"TCP OK({dwell:.2f}s) → 서버 응답: {head!r}"
    except ConnectionResetError:
        dwell = time.time() - t0
        return False, f"TCP OK → WS 업그레이드 단계에서 서버가 RST(Connection reset) ({dwell:.2f}s)"
    except Exception as e:
        dwell = time.time() - t0
        return False, f"TCP OK → 업그레이드 응답 수신 실패 {type(e).__name__}: {e} ({dwell:.2f}s)"
    finally:
        try:
            s.close()
        except Exception:
            pass


async def ws_handshake(url: str, idx: int, open_timeout: float = 10.0) -> bool:
    """websockets 라이브러리로 실제 핸드셰이크(=production SDK 와 동일 경로) 시도."""
    t0 = time.time()
    try:
        async with websockets.connect(
            url, ping_interval=20, ping_timeout=10, open_timeout=open_timeout
        ) as ws:
            dwell = time.time() - t0
            print(f"  [{idx}] ✅ HANDSHAKE OK  dwell={dwell:.2f}s  state={ws.state.name}")
            await ws.close()
            return True
    except Exception as e:
        dwell = time.time() - t0
        print(f"  [{idx}] ❌ FAIL {type(e).__name__}: {e}  dwell={dwell:.2f}s")
        return False


async def main(count: int, interval: float) -> int:
    url = _ws_url()
    host, port = _host_port(url)
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST")

    print("=" * 68)
    print(f"KIS WSS 핸드셰이크 probe  @ {now}")
    print(f"URL = {url}   (host={host}, port={port})")
    print("※ 구독/approval 발급 없음 — 캐시 approval_key 영향 없음")
    print("=" * 68)

    print("\n[1] TCP + raw 업그레이드 응답")
    ok_tcp, msg = tcp_probe(host, port)
    print(f"  {'✅' if ok_tcp else '❌'} {msg}")

    print(f"\n[2] WebSocket 핸드셰이크 × {count} (간격 {interval}s)")
    oks = 0
    for i in range(1, count + 1):
        if await ws_handshake(url, i):
            oks += 1
        if i < count:
            await asyncio.sleep(interval)

    print("\n" + "-" * 68)
    print(f"결과: 핸드셰이크 {oks}/{count} 성공")
    t = datetime.now(KST).time()
    in_market = (t.hour, t.minute) >= (8, 30) and (t.hour, t.minute) <= (15, 40)
    if oks == 0:
        if in_market:
            print("판정: 장중인데 전부 실패 → 연결레벨 차단/throttle 의심 (off-hours 아님)")
        else:
            print("판정: 장 시간대 아님 → off-hours 거부 가능성. 장중 재실행으로 확정 필요")
    else:
        print("판정: 핸드셰이크 정상 → 연결레벨은 문제 없음. 증상은 구독/approval/데이터 계층")
    print("-" * 68)
    return 0 if oks > 0 else 1


if __name__ == "__main__":
    _count = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    _interval = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0
    try:
        sys.exit(asyncio.run(main(_count, _interval)))
    except KeyboardInterrupt:
        print("\n중단됨")
        sys.exit(2)
