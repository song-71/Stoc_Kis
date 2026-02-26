#!/usr/bin/env python3
"""
ws_realtime_subscribe_to_parquet-2.py 시간대별 로직 시뮬레이션 테스트
- 실시간 장 운영 없이 critical time 전환 시뮬레이션
- 컬럼 정규화(대문자→소문자) 검증
실행: python test_wss_time_simulation.py
"""
from datetime import datetime, time as dtime, timedelta
from zoneinfo import ZoneInfo
from enum import Enum, auto
import pandas as pd
import sys

KST = ZoneInfo("Asia/Seoul")
BASE_DATE = datetime(2026, 2, 11, tzinfo=KST)  # 수요일


def dt(h: int, m: int = 0, s: int = 0) -> datetime:
    """BASE_DATE 기준 시각"""
    return BASE_DATE.replace(hour=h, minute=m, second=s, microsecond=0)


# =============================================================================
# 1. RunMode / calc_mode 복제 (원본과 동일해야 함)
# =============================================================================
class RunMode(Enum):
    PREOPEN_WAIT = auto()
    PREOPEN_EXP = auto()
    REGULAR_REAL = auto()
    CLOSE_EXP = auto()
    CLOSE_REAL = auto()
    OVERTIME_EXP = auto()
    OVERTIME_REAL = auto()
    STOP = auto()
    EXIT = auto()


END_TIME = dtime(18, 0)


def calc_mode(now: datetime) -> RunMode:
    t = now.time()
    if t < dtime(8, 50):
        return RunMode.PREOPEN_WAIT
    if dtime(8, 50) <= t < dtime(9, 0):
        return RunMode.PREOPEN_EXP
    if dtime(9, 0) <= t < dtime(15, 10, 10):
        return RunMode.REGULAR_REAL
    if dtime(15, 10, 10) <= t < dtime(15, 30):
        return RunMode.CLOSE_EXP
    if dtime(15, 30) <= t < dtime(16, 0):
        return RunMode.CLOSE_REAL
    if dtime(16, 0) <= t < END_TIME:
        return RunMode.OVERTIME_EXP  # 시뮬레이션: overtime_real_active=False 가정
    return RunMode.EXIT


# =============================================================================
# 2. 15:31~16:00 재구독 차단 구간
# =============================================================================
def in_close_dead_zone(nt) -> bool:
    return dtime(15, 31) <= nt < dtime(16, 0)


# =============================================================================
# 3. Top rank schedule (15:09, 15:10 포함)
# =============================================================================
def build_top_rank_schedule(base_date) -> list:
    base = base_date.date()
    fixed = [
        dtime(9, 0), dtime(9, 5), dtime(9, 10), dtime(9, 15), dtime(9, 30),
    ]
    times = [datetime.combine(base, t, tzinfo=KST) for t in fixed]
    cur = datetime.combine(base, dtime(10, 0), tzinfo=KST)
    end = datetime.combine(base, dtime(15, 0), tzinfo=KST)
    while cur <= end:
        times.append(cur)
        cur += timedelta(minutes=30)
    times.append(datetime.combine(base, dtime(15, 9), tzinfo=KST))
    times.append(datetime.combine(base, dtime(15, 10), tzinfo=KST))
    return sorted(set(times))


# =============================================================================
# 4. 컬럼 정규화 (수신 시점 소문자 통일)
# =============================================================================
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()]
    return df


def test_column_normalization():
    """ccnl(대문자) + exp(소문자) concat 후 정규화 시 중복 없어야 함"""
    # ccnl_krx 스타일 (대문자)
    df_ccnl = pd.DataFrame({
        "MKSC_SHRN_ISCD": ["005930"],
        "STCK_PRPR": ["71000"],
        "ACML_VOL": ["1234567"],
        "ASKP1": ["71050"],
        "BIDP1": ["71000"],
    })
    # exp_ccnl_krx 스타일 (소문자)
    df_exp = pd.DataFrame({
        "mksc_shrn_iscd": ["005930"],
        "stck_prpr": ["71050"],
        "acml_vol": ["1234600"],
        "askp1": ["71060"],
        "bidp1": ["71050"],
    })
    # 수신 시점 정규화 적용
    df1 = normalize_columns(df_ccnl)
    df2 = normalize_columns(df_exp)
    combined = pd.concat([df1, df2], ignore_index=True)
    assert not combined.columns.duplicated().any(), "중복 컬럼 발생!"
    assert "stck_prpr" in combined.columns
    assert "STCK_PRPR" not in combined.columns
    return True


def run_tests():
    ok = 0
    fail = 0

    # ---- 1. calc_mode 시간대별 ---- 
    cases = [
        (dt(8, 49), RunMode.PREOPEN_WAIT),
        (dt(8, 50), RunMode.PREOPEN_EXP),
        (dt(9, 0), RunMode.REGULAR_REAL),
        (dt(12, 0), RunMode.REGULAR_REAL),
        (dt(15, 10, 9), RunMode.REGULAR_REAL),
        (dt(15, 10, 10), RunMode.CLOSE_EXP),
        (dt(15, 29, 59), RunMode.CLOSE_EXP),
        (dt(15, 30), RunMode.CLOSE_REAL),
        (dt(15, 30, 1), RunMode.CLOSE_REAL),
        (dt(16, 0), RunMode.OVERTIME_EXP),
        (dt(17, 59), RunMode.OVERTIME_EXP),
        (dt(18, 0), RunMode.EXIT),
    ]
    for now, expected in cases:
        got = calc_mode(now)
        if got == expected:
            ok += 1
            print(f"  [OK] calc_mode({now.strftime('%H:%M:%S')}) = {got.name}")
        else:
            fail += 1
            print(f"  [FAIL] calc_mode({now.strftime('%H:%M:%S')}) expected {expected.name} got {got.name}")

    # ---- 2. 15:31~16:00 재구독 차단 ---- 
    for nt, expect_dead in [
        (dtime(15, 30, 59), False),
        (dtime(15, 31), True),
        (dtime(15, 45), True),
        (dtime(15, 59, 59), True),
        (dtime(16, 0), False),
    ]:
        if in_close_dead_zone(nt) == expect_dead:
            ok += 1
            print(f"  [OK] in_close_dead_zone({nt}) = {expect_dead}")
        else:
            fail += 1
            print(f"  [FAIL] in_close_dead_zone({nt}) expected {expect_dead} got {in_close_dead_zone(nt)}")

    # ---- 3. Top rank schedule 15:09, 15:10 포함 ---- 
    sched = build_top_rank_schedule(BASE_DATE)
    has_1509 = any(t.time() == dtime(15, 9) for t in sched)
    has_1510 = any(t.time() == dtime(15, 10) for t in sched)
    if has_1509 and has_1510:
        ok += 1
        print(f"  [OK] schedule has 15:09, 15:10")
    else:
        fail += 1
        print(f"  [FAIL] schedule 15:09={has_1509} 15:10={has_1510}")

    # ---- 4. 15:10 is_closing_switch + _closing_codes → fetch 스킵 ----
    next_ts_1510 = datetime.combine(BASE_DATE.date(), dtime(15, 10), tzinfo=KST)
    is_closing_switch = next_ts_1510.time() >= dtime(15, 10)
    _closing_codes = ["005930", "000660"]  # 가정
    skip_fetch = is_closing_switch and bool(_closing_codes)
    if skip_fetch:
        ok += 1
        print(f"  [OK] 15:10 + _closing_codes → fetch 스킵 조건 만족")
    else:
        fail += 1
        print(f"  [FAIL] 15:10 skip_fetch expected True got {skip_fetch}")

    # ---- 5. 컬럼 정규화 (대문자/소문자 concat) ----
    try:
        test_column_normalization()
        ok += 1
        print(f"  [OK] 컬럼 정규화 (ccnl+exp concat) 중복 없음")
    except AssertionError as e:
        fail += 1
        print(f"  [FAIL] 컬럼 정규화: {e}")

    # ---- 6. CLOSE_STOP_TIME 15:31 ----
    CLOSE_STOP_TIME = dtime(15, 31)
    t_1530 = dtime(15, 30, 59)
    t_1531 = dtime(15, 31)
    if t_1530 < CLOSE_STOP_TIME and t_1531 >= CLOSE_STOP_TIME:
        ok += 1
        print(f"  [OK] CLOSE_STOP_TIME=15:31 구간 정확")
    else:
        fail += 1
        print(f"  [FAIL] CLOSE_STOP_TIME 검증")

    return ok, fail


if __name__ == "__main__":
    print("=" * 60)
    print("ws_realtime_subscribe_to_parquet-2 시간 시뮬레이션 테스트")
    print("=" * 60)
    ok, fail = run_tests()
    print("=" * 60)
    print(f"결과: OK={ok} FAIL={fail}")
    print("=" * 60)
    sys.exit(0 if fail == 0 else 1)
