#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
시장데이터 공유메모리 버스 (market_bus) — 호가/체결 실시간 단일출처
======================================================================

목적
----
시장데이터(체결·호가)는 거래 주체와 무관하게 동일하므로 **한 번만 수신**해서
공유메모리에 올리고, 전략·실행 소비자(프로덕션, 여러 계좌/사람)가 그걸 읽어 쓴다.

구조 (1프로세스=1WSS연결 제약 우회 + 다계좌 수평확장 토대)
-----------------------------------------------------------
  [생산자 프로세스들]                   [공유메모리 버스]          [소비자들]
   a2 호가 수신  ─┐
   b1 체결 수신  ─┼─▶  종목별 슬롯(seqlock, latest-wins)  ─▶  전략/실행(N계좌)
   b2 호가 수신  ─┘
- 생산자는 자기 샤드(담당 종목) 슬롯만 갱신. 종목 늘면 생산자(계좌 연결) 추가.
- 소비자는 버스 하나만 읽어 전 종목 최신 시장상태 확보(계좌 수와 무관).

동시성 — seqlock (락 없음, 생산자 비차단)
-----------------------------------------
- 쓰기: seq 를 홀수로(+1) → 데이터 기록 → 짝수로(+1). "쓰는 중" = 홀수.
- 읽기: seq 읽고(짝수여야 함) → 데이터 읽고 → seq 다시 읽어 동일하면 유효, 아니면 재시도.
- latest-wins: 생산자는 소비자를 기다리지 않고 항상 최신값으로 덮어쓴다(백프레셔 없음).
- (참고) CPython + x86 강한 메모리모델 + seqlock 더블체크로 실무상 torn read 를 막는다.
  드물게 재시도가 한 번 더 도는 정도이며, 시장데이터(최신상태)엔 충분히 안전하다.

구독목록 변경 — generation
--------------------------
- 생산자가 set_codes() 로 슬롯↔종목 배정을 바꾸면 헤더 generation 을 올린다.
- 소비자는 generation 변화를 감지하면 code→슬롯 매핑을 자동 재구성한다(휴장 넘어가며
  종목이 바뀌어도 무중단).

사용 예
-------
  # 생산자(예: 호가 수신 프로세스)
  bus = MarketDataBus.create("stockbus", capacity=256)
  bus.set_codes(["000400", "001210", ...])
  bus.update_orderbook("000400", recv_epoch, bsop_hour="091922",
                       askp=[...10...], bidp=[...10...],
                       askp_rsqn=[...10...], bidp_rsqn=[...10...],
                       total_askp_rsqn=..., total_bidp_rsqn=...)

  # 소비자(예: 프로덕션 전략)
  bus = MarketDataBus.attach("stockbus")
  ob = bus.get("000400")     # {recv_epoch, askp[10], bidp[10], ...} 또는 None

  # 종료 시 생산자만 unlink
  bus.close(unlink=True)
"""
from __future__ import annotations

import numpy as np
from multiprocessing import shared_memory

# ── 레이아웃 상수 ────────────────────────────────────────────────────────────
MAGIC = 0x4F424B31          # 'OBK1'
VERSION = 1
HEADER_BYTES = 64           # 슬롯 시작 오프셋(헤더 영역, 고정)
DEPTH = 10                  # 호가 단계 수
DEFAULT_CAPACITY = 256      # 최대 종목 수(유니버스 성장 여유)

HDR_DTYPE = np.dtype([
    ("magic",      np.uint32),
    ("version",    np.uint32),
    ("capacity",   np.uint32),
    ("n_active",   np.uint32),
    ("generation", np.uint64),   # 구독목록(슬롯 배정) 변경 시 증가
])

# 슬롯 = 종목별 최신 시장상태(현재는 호가 중심, 체결 필드 확장 여지)
SLOT_DTYPE = np.dtype([
    ("seq",            np.uint64),            # seqlock 카운터(짝수=안정, 홀수=쓰는중)
    ("code",           "S7"),                 # 종목코드(zfill6 + 여유)
    ("name",           "S60"),                # 종목명(utf-8)
    ("recv_epoch",     np.float64),           # 수신시각(KST epoch, time.time())
    ("bsop_hour",      "S6"),                 # 거래소 시각 HHMMSS
    ("askp",           np.int32, (DEPTH,)),   # 매도호가 1~10
    ("bidp",           np.int32, (DEPTH,)),   # 매수호가 1~10
    ("askp_rsqn",      np.int64, (DEPTH,)),   # 매도잔량 1~10
    ("bidp_rsqn",      np.int64, (DEPTH,)),   # 매수잔량 1~10
    ("total_askp_rsqn", np.int64),
    ("total_bidp_rsqn", np.int64),
])

_READ_RETRIES = 8           # seqlock 읽기 재시도 한도


class MarketDataBus:
    """공유메모리 시장데이터 버스. create()(생산자) / attach()(소비자)로 생성."""

    def __init__(self, shm: shared_memory.SharedMemory, capacity: int):
        self._shm = shm
        self._capacity = capacity
        self._hdr = np.ndarray((1,), dtype=HDR_DTYPE, buffer=shm.buf, offset=0)
        self._slots = np.ndarray((capacity,), dtype=SLOT_DTYPE,
                                 buffer=shm.buf, offset=HEADER_BYTES)
        self._idx: dict[str, int] = {}     # code -> slot index (로컬 캐시)
        self._seen_gen: int = -1           # 마지막으로 매핑 재구성한 generation

    # ── 생성/접속 ────────────────────────────────────────────────────────
    @classmethod
    def _bytes(cls, capacity: int) -> int:
        return HEADER_BYTES + capacity * SLOT_DTYPE.itemsize

    @classmethod
    def create(cls, name: str, capacity: int = DEFAULT_CAPACITY) -> "MarketDataBus":
        """생산자용. 기존 동명 블록이 있으면 정리 후 새로 만든다."""
        size = cls._bytes(capacity)
        try:
            old = shared_memory.SharedMemory(name=name)
            old.close(); old.unlink()
        except FileNotFoundError:
            pass
        shm = shared_memory.SharedMemory(name=name, create=True, size=size)
        self = cls(shm, capacity)
        self._hdr[0]["magic"] = MAGIC
        self._hdr[0]["version"] = VERSION
        self._hdr[0]["capacity"] = capacity
        self._hdr[0]["n_active"] = 0
        self._hdr[0]["generation"] = 0
        self._slots["seq"] = 0
        return self

    @classmethod
    def attach(cls, name: str) -> "MarketDataBus":
        """소비자용. 기존 블록에 붙는다."""
        shm = shared_memory.SharedMemory(name=name)
        hdr = np.ndarray((1,), dtype=HDR_DTYPE, buffer=shm.buf, offset=0)
        if int(hdr[0]["magic"]) != MAGIC:
            shm.close()
            raise ValueError(f"market_bus '{name}': magic 불일치(버스 아님/손상)")
        cap = int(hdr[0]["capacity"])
        self = cls(shm, cap)
        self._refresh_map()
        return self

    # ── 슬롯↔종목 배정 (생산자) ───────────────────────────────────────────
    def set_codes(self, codes: list[str]) -> None:
        """구독 종목을 슬롯 0..n-1 에 배정하고 generation 을 올린다."""
        codes = [str(c).strip().zfill(6) for c in codes if str(c).strip()]
        if len(codes) > self._capacity:
            raise ValueError(f"종목 {len(codes)} > capacity {self._capacity}")
        for i, code in enumerate(codes):
            # 슬롯 코드 변경도 seqlock 보호(소비자가 읽는 중일 수 있음).
            # 재배정 슬롯의 이전 데이터를 비워 stale 읽기를 막는다(recv_epoch=0 → 미수신).
            seq = int(self._slots["seq"][i])
            self._slots["seq"][i] = seq + 1
            self._slots["code"][i] = code.encode()[:7]
            self._slots["recv_epoch"][i] = 0.0
            self._slots["name"][i] = b""
            self._slots["seq"][i] = seq + 2
        self._hdr[0]["n_active"] = len(codes)
        self._hdr[0]["generation"] = int(self._hdr[0]["generation"]) + 1
        self._idx = {c: i for i, c in enumerate(codes)}
        self._seen_gen = int(self._hdr[0]["generation"])

    def _refresh_map(self) -> None:
        """소비자: generation 기준으로 code→슬롯 매핑 재구성."""
        n = int(self._hdr[0]["n_active"])
        idx = {}
        for i in range(n):
            c = bytes(self._slots["code"][i]).split(b"\x00", 1)[0].decode("ascii", "ignore")
            if c:
                idx[c] = i
        self._idx = idx
        self._seen_gen = int(self._hdr[0]["generation"])

    def _ensure_map(self) -> None:
        if int(self._hdr[0]["generation"]) != self._seen_gen:
            self._refresh_map()

    # ── 쓰기 (생산자) ─────────────────────────────────────────────────────
    def update_orderbook(self, code: str, recv_epoch: float, *,
                         bsop_hour: str = "",
                         askp, bidp, askp_rsqn, bidp_rsqn,
                         total_askp_rsqn: int = 0, total_bidp_rsqn: int = 0,
                         name: str = "") -> bool:
        """종목 호가 슬롯을 seqlock 으로 갱신. 미배정 코드면 False."""
        i = self._idx.get(str(code).zfill(6))
        if i is None:
            return False
        s = self._slots
        seq = int(s["seq"][i])
        s["seq"][i] = seq + 1                 # 쓰기 시작(홀수)
        s["recv_epoch"][i] = recv_epoch
        s["bsop_hour"][i] = str(bsop_hour).encode()[:6]
        if name:
            s["name"][i] = name.encode("utf-8")[:60]
        s["askp"][i] = np.asarray(askp, dtype=np.int32)[:DEPTH]
        s["bidp"][i] = np.asarray(bidp, dtype=np.int32)[:DEPTH]
        s["askp_rsqn"][i] = np.asarray(askp_rsqn, dtype=np.int64)[:DEPTH]
        s["bidp_rsqn"][i] = np.asarray(bidp_rsqn, dtype=np.int64)[:DEPTH]
        s["total_askp_rsqn"][i] = int(total_askp_rsqn)
        s["total_bidp_rsqn"][i] = int(total_bidp_rsqn)
        s["seq"][i] = seq + 2                 # 쓰기 완료(짝수)
        return True

    # ── 읽기 (소비자) ─────────────────────────────────────────────────────
    def get(self, code: str) -> dict | None:
        """종목 최신 호가를 seqlock 으로 안정 읽기. 없거나 미수신이면 None."""
        self._ensure_map()
        i = self._idx.get(str(code).zfill(6))
        if i is None:
            return None
        s = self._slots
        for _ in range(_READ_RETRIES):
            seq1 = int(s["seq"][i])
            if seq1 & 1:                      # 쓰는 중(홀수) → 재시도
                continue
            if seq1 == 0:                     # 슬롯 미초기화 → 미수신
                return None
            d = {
                "code": bytes(s["code"][i]).split(b"\x00", 1)[0].decode("ascii", "ignore"),
                "name": bytes(s["name"][i]).split(b"\x00", 1)[0].decode("utf-8", "ignore"),
                "recv_epoch": float(s["recv_epoch"][i]),
                "bsop_hour": bytes(s["bsop_hour"][i]).split(b"\x00", 1)[0].decode("ascii", "ignore"),
                "askp": s["askp"][i].tolist(),
                "bidp": s["bidp"][i].tolist(),
                "askp_rsqn": s["askp_rsqn"][i].tolist(),
                "bidp_rsqn": s["bidp_rsqn"][i].tolist(),
                "total_askp_rsqn": int(s["total_askp_rsqn"][i]),
                "total_bidp_rsqn": int(s["total_bidp_rsqn"][i]),
            }
            if int(s["seq"][i]) == seq1:      # 읽는 중 안 바뀜 → 유효
                return d if d["recv_epoch"] > 0.0 else None   # recv_epoch=0 → 호가 미수신
        return None                           # 경합 과다(드묾) → 다음 틱에 재시도

    def snapshot_all(self) -> dict[str, dict]:
        self._ensure_map()
        out = {}
        for c in list(self._idx.keys()):
            d = self.get(c)
            if d is not None:
                out[c] = d
        return out

    @property
    def codes(self) -> list[str]:
        self._ensure_map()
        return list(self._idx.keys())

    def close(self, unlink: bool = False) -> None:
        try:
            self._hdr = None
            self._slots = None
            self._shm.close()
            if unlink:
                self._shm.unlink()
        except Exception:
            pass


# ── 자체 검증 ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import time

    name = "market_bus_selftest"
    w = MarketDataBus.create(name, capacity=8)
    w.set_codes(["000400", "001210", "002990"])
    r = MarketDataBus.attach(name)

    # 1) 미수신 슬롯
    assert r.get("000400") is None, "미수신은 None 이어야"

    # 2) 갱신 후 읽기
    w.update_orderbook("000400", time.time(), bsop_hour="091922",
                       askp=list(range(2610, 2620)), bidp=list(range(2600, 2590, -1)),
                       askp_rsqn=list(range(10)), bidp_rsqn=list(range(100, 110)),
                       total_askp_rsqn=12345, total_bidp_rsqn=67890, name="롯데손해보험")
    d = r.get("000400")
    assert d is not None and d["askp"][0] == 2610 and d["bidp"][0] == 2600, d
    assert d["name"] == "롯데손해보험" and d["total_askp_rsqn"] == 12345, d
    print("[1/4] 쓰기→읽기 OK:", d["code"], d["name"], "askp1=", d["askp"][0], "bidp1=", d["bidp"][0])

    # 3) latest-wins 덮어쓰기
    for px in range(2611, 2616):
        w.update_orderbook("000400", time.time(), bsop_hour="091925",
                           askp=[px]*10, bidp=[px-5]*10,
                           askp_rsqn=[1]*10, bidp_rsqn=[1]*10)
    d = r.get("000400")
    assert d["askp"][0] == 2615, d
    print("[2/4] latest-wins OK: askp1=", d["askp"][0])

    # 4) 미배정 코드
    assert r.get("999999") is None
    print("[3/4] 미배정 코드 None OK")

    # 5) generation 재구성(구독 변경)
    w.set_codes(["005930", "000660"])     # 종목 교체
    assert r.get("000400") is None, "교체 후 옛 코드 매핑 사라져야"
    w.update_orderbook("005930", time.time(), bsop_hour="100000",
                       askp=[70000]*10, bidp=[69900]*10, askp_rsqn=[5]*10, bidp_rsqn=[5]*10)
    d = r.get("005930")
    assert d is not None and d["askp"][0] == 70000, d
    print("[4/4] generation 재구성 OK: 새 종목 005930 askp1=", d["askp"][0], "| codes=", r.codes)

    r.close()
    w.close(unlink=True)
    print("market_bus 자체검증 통과 ✓")
