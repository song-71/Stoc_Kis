"""WSS 틱 parquet 병합 공용 유틸 — recv_ts 틱 유일화 dedup.

배경(260706 진단):
    한 WSS 프레임에는 국내주식 실시간체결(H0STCNT0)이 **최대 20건**까지 묶여 온다.
    그런데 프로덕션은 recv_ts 를 프레임당 1번만 찍어(ws_realtime_trading.py:12788→:12852)
    그 20행이 **모두 같은 recv_ts** 를 갖는다(구 6자리 데이터).
    이 상태에서 `drop_duplicates(subset=[code, recv_ts])` 로 병합하면 서로 다른 실체결
    19건이 "중복"으로 삭제된다(하루 수신의 약 54% 소실 실측).

이 유틸은 그 손실 없이 병합하도록:
    1) **완전히 동일한 행**(같은 part 를 두 번 병합한 진짜 중복)만 제거하고,
    2) 같은 (code, recv_ts) 로 묶인 **서로 다른 실체결**은 삭제하지 않고,
       수신순(acml_vol 누적거래량 오름차순 = 거래소 체결순)으로 순번을 매겨
       recv_ts 소수점 뒤에 자리수(기본 2자리, 그룹 최대크기에 맞춰 확장)를 붙여
       유일화한다(6자리 → 8자리).

신규 데이터(2026-07-06 이후 ingest 에서 이미 8자리로 유일화된 것)는 (code, recv_ts)
중복 그룹이 없으므로 순번 부여를 건너뛰고 그대로 통과한다(멱등).

문자열 정렬·차트(str.slice 고정위치)·pandas to_datetime(나노초 보존) 모두 안전하다.
"""
from __future__ import annotations

_CODE_COLS = ("mksc_shrn_iscd", "stck_shrn_iscd", "code")
_ACML_COLS = ("acml_vol", "ACML_VOL")


def unique_recv_ts(df):
    """recv_ts 를 틱마다 유일하게 만든다.

    Parameters
    ----------
    df : pandas.DataFrame  (recv_ts 컬럼 포함)

    Returns
    -------
    (df, n_fullrow_removed, n_disambiguated)
        df               : 처리된 DataFrame (recv_ts 오름차순 정렬)
        n_fullrow_removed: 완전 동일 행으로 제거된 진짜 중복 수
        n_disambiguated  : 같은 (code, recv_ts) 그룹에 속해 순번으로 유일화된 행 수
    """
    import pandas as pd

    if "recv_ts" not in df.columns or len(df) == 0:
        return df, 0, 0

    # 1) 완전 동일 행 제거 (재병합 진짜 중복만)
    before = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    n_full = before - len(df)

    code_col = next((c for c in _CODE_COLS if c in df.columns), None)
    if code_col is None:
        # 종목 컬럼이 없으면 순번 부여 불가 → 정렬만
        return df.sort_values("recv_ts", kind="mergesort").reset_index(drop=True), n_full, 0

    key = [code_col, "recv_ts"]
    dup_mask = df.duplicated(subset=key, keep=False)
    n_dis = int(dup_mask.sum())

    if n_dis:
        # 수신순 = acml_vol 오름차순(거래소 체결순). 없으면 기존 행순서 유지.
        av = next((c for c in _ACML_COLS if c in df.columns), None)
        if av is not None:
            df["__av"] = pd.to_numeric(df[av], errors="coerce")
            df = df.sort_values(key + ["__av"], kind="mergesort").reset_index(drop=True)
            df = df.drop(columns="__av")
        else:
            df = df.sort_values(key, kind="mergesort").reset_index(drop=True)

        # dropna=False: code/recv_ts 가 NaN 인 쓰레기 행도 그룹으로 유지해 cumcount 가
        # int64 로 유지되게 한다(dropna=True 면 NaN 키가 제외돼 결과가 float→ ".0" 오염).
        seq = df.groupby(key, sort=False, dropna=False).cumcount().astype("int64")
        width = max(2, len(str(int(seq.max()))))  # 그룹 최대크기에 맞춰 자리수 확장(기본 2)
        df["recv_ts"] = df["recv_ts"].astype(str) + seq.map(lambda i: str(int(i)).zfill(width))

    df = df.sort_values("recv_ts", kind="mergesort").reset_index(drop=True)
    return df, n_full, n_dis
