"""
KIS OHLCV 유틸리티 => 매일 데이터 다운로드할 때 사용하는 필수 모듈임.(삭제 금지)

- kis_1d_ohlcv_Daily_fetch_manager.py 프로그램에서 다운로드에 필요한 필수 기능을 제공합니다.
 -> 1d만 지원, 단, 1m은 별도로 구성
"""
from __future__ import annotations

import json
import os
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
import duckdb
import pandas as pd
import requests
from dateutil.parser import parse as dtparse

from kis_utils import KisDataLayout, load_checkpoints, load_kis_data_layout, send_telegram, update_checkpoint

DEFAULT_BASE_URL = "https://openapi.koreainvestment.com:9443"
logging.basicConfig(level=logging.INFO)

# KIS API 토큰 만료시각은 항상 KST 기준
_KST = timezone(timedelta(hours=9))

def _now_kst_naive() -> datetime:
    """현재 시각을 KST 기준 naive datetime으로 반환 (KIS 토큰 만료시각과 비교용)."""
    return datetime.now(_KST).replace(tzinfo=None)


@dataclass
class KisConfig:
    appkey: str
    appsecret: str
    base_url: str = DEFAULT_BASE_URL
    custtype: str = "P"
    market_div: str = "J"
    token_cache_path: str = "./kis_token.json"
    timeout_sec: int = 10
    max_http_retries: int = 4
    http_backoff_sec: float = 0.7


class KisClient:
    def __init__(self, cfg: KisConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.access_token: Optional[str] = None
        self.token_expire_dt: Optional[datetime] = None

    def _load_cached_token(self) -> bool:
        path = self.cfg.token_cache_path
        if not os.path.exists(path):
            return False
        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)
            token = obj.get("access_token")
            exp = obj.get("access_token_token_expired")
            if not token or not exp:
                return False
            exp_dt = dtparse(exp)  # KIS 만료시각은 KST 기준 naive
            now = _now_kst_naive()
            if exp_dt <= now + timedelta(minutes=5):
                logging.info("[KIS][token] cached token EXPIRED (exp=%s, now_kst=%s) → 재발급",
                             exp_dt, now.strftime("%Y-%m-%d %H:%M:%S"))
                return False
            self.access_token = token
            self.token_expire_dt = exp_dt
            logging.info("[KIS][token] cached token loaded, expires at %s (now_kst=%s)",
                         exp_dt, now.strftime("%Y-%m-%d %H:%M:%S"))
            return True
        except Exception:
            return False

    def _save_token(self, resp_json: dict) -> None:
        with open(self.cfg.token_cache_path, "w", encoding="utf-8") as f:
            json.dump(resp_json, f, ensure_ascii=False, indent=2)

    def ensure_token(self) -> str:
        now = _now_kst_naive()
        if self.access_token and self.token_expire_dt:
            if self.token_expire_dt > now + timedelta(minutes=5):
                return self.access_token

        if self._load_cached_token():
            return self.access_token  # type: ignore

        url = f"{self.cfg.base_url}/oauth2/token"
        data = {
            "grant_type": "client_credentials",
            "appkey": self.cfg.appkey,
            "appsecret": self.cfg.appsecret,
        }

        last_err = None
        for attempt in range(1, self.cfg.max_http_retries + 1):
            try:
                logging.info("[KIS][token] request start attempt=%d/%d", attempt, self.cfg.max_http_retries)
                r = self.session.post(url, data=data, timeout=self.cfg.timeout_sec)
                r.raise_for_status()
                j = r.json()
                if "access_token" not in j:
                    raise RuntimeError(f"Token 발급 실패: {j}")

                self.access_token = j["access_token"]
                # KIS 응답에 access_token_token_expired가 있으면 그대로 사용 (KST)
                # 없으면 expires_in 기준으로 KST 계산
                if "access_token_token_expired" in j:
                    self.token_expire_dt = dtparse(j["access_token_token_expired"])
                else:
                    expires_in = int(j.get("expires_in", 3600))
                    self.token_expire_dt = _now_kst_naive() + timedelta(seconds=expires_in)
                    j["access_token_token_expired"] = self.token_expire_dt.strftime("%Y-%m-%d %H:%M:%S")
                self._save_token(j)
                logging.info(
                    "[KIS][token] issued successfully, expires at %s (KST)",
                    self.token_expire_dt.strftime("%Y-%m-%d %H:%M:%S"),
                )
                return self.access_token
            except Exception as e:
                last_err = e
                sleep = self.cfg.http_backoff_sec * (2 ** (attempt - 1))
                print(f"[KIS][token] attempt={attempt}/{self.cfg.max_http_retries} failed: {e} (sleep {sleep:.1f}s)")
                time.sleep(sleep)

        raise RuntimeError(f"Token 발급 최종 실패: {last_err}")

    def _headers(self, tr_id: str) -> Dict[str, str]:
        token = self.ensure_token()
        return {
            "authorization": f"Bearer {token}",
            "appkey": self.cfg.appkey,
            "appsecret": self.cfg.appsecret,
            "tr_id": tr_id,
            "custtype": self.cfg.custtype,
        }

    def inquire_daily_itemchartprice(
        self,
        code: str,
        start_date: str,
        end_date: str,
        period: str = "D",
        adj: str = "0",
    ) -> pd.DataFrame:
        url = f"{self.cfg.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        headers = self._headers(tr_id="FHKST03010100")
        params = {
            "FID_COND_MRKT_DIV_CODE": self.cfg.market_div,
            "FID_INPUT_ISCD": code,
            "FID_INPUT_DATE_1": start_date,
            "FID_INPUT_DATE_2": end_date,
            "FID_PERIOD_DIV_CODE": period,
            "FID_ORG_ADJ_PRC": adj,
        }

        last_err = None
        for attempt in range(1, self.cfg.max_http_retries + 1):
            try:
                r = self.session.get(url, headers=headers, params=params, timeout=self.cfg.timeout_sec)
                r.raise_for_status()
                j = r.json()
                if str(j.get("rt_cd")) != "0":
                    raise RuntimeError(f"KIS 오류: code={code}, msg={j.get('msg1')}, raw={j}")

                rows = j.get("output2") or []
                if not rows:
                    return pd.DataFrame()
                df = pd.DataFrame(rows)
                if "stck_bsop_date" in df.columns:
                    df["stck_bsop_date"] = pd.to_datetime(df["stck_bsop_date"], errors="coerce")
                    df = df.dropna(subset=["stck_bsop_date"]).sort_values("stck_bsop_date").reset_index(drop=True)
                return df
            except Exception as e:
                last_err = e
                sleep = self.cfg.http_backoff_sec * (2 ** (attempt - 1))
                print(f"[KIS][1d] code={code} attempt={attempt}/{self.cfg.max_http_retries} failed: {e} (sleep {sleep:.1f}s)")
                time.sleep(sleep)

        raise RuntimeError(f"KIS 일봉 호출 최종 실패: {last_err}")

    def inquire_time_dailychartprice(
        self,
        code: str,
        input_date: str,
        input_hour: str,
        incu_yn: str = "Y",
        fake_tick_yn: str = "",
    ) -> pd.DataFrame:
        """
        국내주식 주식일별분봉조회 (과거 분봉)
        """
        url = f"{self.cfg.base_url}/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice"
        headers = self._headers(tr_id="FHKST03010230")
        params = {
            "FID_COND_MRKT_DIV_CODE": self.cfg.market_div,
            "FID_INPUT_ISCD": code,
            "FID_INPUT_HOUR_1": input_hour,
            "FID_INPUT_DATE_1": input_date,
            "FID_PW_DATA_INCU_YN": incu_yn,
            "FID_FAKE_TICK_INCU_YN": fake_tick_yn,
        }

        last_err = None
        for attempt in range(1, self.cfg.max_http_retries + 1):
            try:
                r = self.session.get(url, headers=headers, params=params, timeout=self.cfg.timeout_sec)
                r.raise_for_status()
                j = r.json()
                if str(j.get("rt_cd")) != "0":
                    raise RuntimeError(f"KIS 오류: code={code}, msg={j.get('msg1')}, raw={j}")

                rows = j.get("output2") or []
                if not rows:
                    return pd.DataFrame()
                return pd.DataFrame(rows)
            except Exception as e:
                last_err = e
                sleep = self.cfg.http_backoff_sec * (2 ** (attempt - 1))
                print(f"[KIS][1m] code={code} attempt={attempt}/{self.cfg.max_http_retries} failed: {e} (sleep {sleep:.1f}s)")
                time.sleep(sleep)

        raise RuntimeError(f"KIS 분봉 호출 최종 실패: {last_err}")

    def inquire_price(self, code: str) -> Dict[str, Any]:
        """
        국내주식 현재가 종합정보
        """
        url = f"{self.cfg.base_url}/uapi/domestic-stock/v1/quotations/inquire-price"
        headers = self._headers(tr_id="FHKST01010100")
        params = {
            "FID_COND_MRKT_DIV_CODE": self.cfg.market_div,
            "FID_INPUT_ISCD": code,
        }

        last_err = None
        for attempt in range(1, self.cfg.max_http_retries + 1):
            try:
                r = self.session.get(url, headers=headers, params=params, timeout=self.cfg.timeout_sec)
                r.raise_for_status()
                j = r.json()
                if str(j.get("rt_cd")) != "0":
                    raise RuntimeError(f"KIS 오류: code={code}, msg={j.get('msg1')}, raw={j}")
                output = j.get("output") or {}
                if not isinstance(output, dict):
                    raise RuntimeError(f"KIS 응답 형식 오류: {j}")
                return output
            except Exception as e:
                last_err = e
                sleep = self.cfg.http_backoff_sec * (2 ** (attempt - 1))
                print(f"[KIS][price] code={code} attempt={attempt}/{self.cfg.max_http_retries} failed: {e} (sleep {sleep:.1f}s)")
                time.sleep(sleep)

        raise RuntimeError(f"KIS 현재가 호출 최종 실패: {last_err}")

STANDARD_1D_COLS = ["date", "symbol", "open", "high", "low", "close", "volume", "value"]
STANDARD_1M_COLS = ["date", "ts", "symbol", "open", "high", "low", "close", "volume", "value"]

KIS_1D_MAP_CANDIDATES = {
    "open": ["stck_oprc", "open", "oprc", "stck_oprc_prpr"],
    "high": ["stck_hgpr", "high", "hgpr"],
    "low": ["stck_lwpr", "low", "lwpr"],
    "close": ["stck_clpr", "close", "clpr", "stck_prpr"],
    "volume": ["acml_vol", "volume", "vol"],
    "value": ["acml_tr_pbmn", "acml_tr_pbmn_amt", "value", "tr_pbmn"],
    "date": ["stck_bsop_date", "date"],
}

KIS_1M_MAP_CANDIDATES = {
    "date": ["stck_bsop_date", "bsop_date", "date"],
    "time": ["stck_cntg_hour", "cntg_hour", "time", "stck_cntg_time"],
    "open": ["stck_oprc", "open", "oprc", "stck_oprc_prpr"],
    "high": ["stck_hgpr", "high", "hgpr"],
    "low": ["stck_lwpr", "low", "lwpr"],
    "close": ["stck_clpr", "close", "clpr", "stck_prpr"],
    "volume": ["cntg_vol", "acml_vol", "volume", "vol"],
    "value": ["acml_tr_pbmn", "acml_tr_pbmn_amt", "value", "tr_pbmn"],
}


def _pick_first_existing(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def standardize_1d_df(raw: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=STANDARD_1D_COLS)

    date_col = _pick_first_existing(raw, KIS_1D_MAP_CANDIDATES["date"])
    if not date_col:
        raise RuntimeError(f"[standardize_1d_df] date column not found. columns={list(raw.columns)[:50]}")

    col_map: Dict[str, str] = {}
    for std_col, cands in KIS_1D_MAP_CANDIDATES.items():
        src = _pick_first_existing(raw, cands)
        if src:
            col_map[std_col] = src

    df = raw.copy()
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col])

    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df[col_map["date"]], errors="coerce").dt.strftime("%Y-%m-%d")
    out["symbol"] = symbol

    def to_num(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series, errors="coerce")

    for std in ["open", "high", "low", "close", "volume", "value"]:
        src = col_map.get(std)
        if src and src in df.columns:
            out[std] = to_num(df[src])
        else:
            out[std] = pd.NA

    out = out.dropna(subset=["date"]).drop_duplicates(subset=["date", "symbol"], keep="last")
    out = out.sort_values(["date", "symbol"]).reset_index(drop=True)

    for c in ["open", "high", "low", "close", "value"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("float64")
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce").astype("float64")

    return out[STANDARD_1D_COLS]


def standardize_1m_df(raw: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=STANDARD_1M_COLS)

    date_col = _pick_first_existing(raw, KIS_1M_MAP_CANDIDATES["date"])
    time_col = _pick_first_existing(raw, KIS_1M_MAP_CANDIDATES["time"])
    if not date_col or not time_col:
        raise RuntimeError(f"[standardize_1m_df] date/time column not found. columns={list(raw.columns)[:50]}")

    col_map: Dict[str, str] = {}
    for std_col, cands in KIS_1M_MAP_CANDIDATES.items():
        if std_col in ("date", "time"):
            continue
        src = _pick_first_existing(raw, cands)
        if src:
            col_map[std_col] = src

    df = raw.copy()
    df[date_col] = df[date_col].astype(str)
    df[time_col] = df[time_col].astype(str).str.zfill(6)
    ts = pd.to_datetime(
        df[date_col] + df[time_col],
        format="%Y%m%d%H%M%S",
        errors="coerce",
    )
    df = df.loc[ts.notna()].copy()
    df["ts"] = ts.dt.strftime("%Y-%m-%d %H:%M:%S")
    df["date"] = ts.dt.strftime("%Y-%m-%d")

    out = pd.DataFrame()
    out["date"] = df["date"]
    out["ts"] = df["ts"]
    out["symbol"] = symbol

    def to_num(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series, errors="coerce")

    for std in ["open", "high", "low", "close", "volume", "value"]:
        src = col_map.get(std)
        if src and src in df.columns:
            out[std] = to_num(df[src])
        else:
            out[std] = pd.NA

    out = out.dropna(subset=["date", "ts"]).drop_duplicates(subset=["date", "ts", "symbol"], keep="last")
    out = out.sort_values(["date", "ts", "symbol"]).reset_index(drop=True)
    return out[STANDARD_1M_COLS]


# =============================================================================
# TEMP -> OUT -> S3 -> CLEANUP
# =============================================================================


def _list_temp_parquet_files(temp_dir: str) -> List[str]:
    if not os.path.exists(temp_dir):
        return []
    out = []
    for name in os.listdir(temp_dir):
        if name.endswith(".parquet"):
            out.append(os.path.join(temp_dir, name))
    return sorted(out)


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _s3_split(s3_url: str) -> tuple[str, str]:
    if not s3_url.startswith("s3://"):
        raise ValueError(f"s3 url 형식이 아닙니다: {s3_url}")
    without = s3_url[5:]
    parts = without.split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"s3 url 파싱 실패: {s3_url}")
    return parts[0], parts[1]


def _list_latest_date_from_s3(layout: KisDataLayout) -> Optional[str]:
    s3 = boto3.client("s3")
    prefix = f"{layout.s3_prefix}/market_data/1d/"
    paginator = s3.get_paginator("list_objects_v2")
    latest: Optional[str] = None
    for page in paginator.paginate(Bucket=layout.s3_bucket, Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                key = obj.get("Key", "")
            if "date=" not in key:
                    continue
            # .../date=YYYY-MM-DD/ohlcv.parquet
            try:
                part = key.split("date=", 1)[1]
                date_str = part.split("/", 1)[0]
                if len(date_str) == 10:
                    if latest is None or date_str > latest:
                        latest = date_str
            except Exception:
                continue
    return latest


def _update_checkpoint_config(config_path: str, name: str, latest_date: str) -> None:
    update_checkpoint(config_path, name, latest_date)
    print(f"[checkpoint] updated: {name} -> {latest_date}")


def compact_temp_to_out(layout: KisDataLayout, freq: str) -> List[str]:
    """
    TEMP(by_symbol/1d) -> OUT(market_data/1d/date=.../ohlcv.parquet)
    Returns: 생성된 날짜 리스트

    DuckDB 조회 예시:
    SELECT *
    FROM read_parquet('s3://tfttrain/KIS_DB/market_data/1d/date=*/ohlcv.parquet', hive_partitioning=1)
    WHERE date BETWEEN '2025-12-01' AND '2025-12-31'
      AND symbol IN ('005930','000660')
    ORDER BY symbol, date;
    """
    temp_dir = layout.local_temp_by_symbol_1d() if freq == "1d" else layout.local_temp_by_symbol_1m()
    files = _list_temp_parquet_files(temp_dir)
    if not files:
        print(f"[OUT] TEMP 파일 없음: {temp_dir}")
        return []

    con = duckdb.connect()
    try:
        files_sql = str(files).replace("'", '"')
        con.execute(f"CREATE OR REPLACE TABLE temp_data AS SELECT * FROM read_parquet({files_sql})")
        dates = con.execute("SELECT DISTINCT date FROM temp_data ORDER BY date").fetchall()
        date_list = [d[0] for d in dates]
        if not date_list:
            print("[OUT] TEMP 데이터에 date 없음")
            return []

        for d in date_list:
            out_path = layout.local_out_1d_date(d) if freq == "1d" else layout.local_out_1m_date(d)
            _ensure_dir(os.path.dirname(out_path))
            if freq == "1d":
                df = con.execute(
                    "SELECT date, symbol, open, high, low, close, volume, value "
                    "FROM temp_data WHERE date = ? ORDER BY symbol, date",
                    [d],
                ).df()
            else:
                df = con.execute(
                    "SELECT date, ts, symbol, open, high, low, close, volume, value "
                    "FROM temp_data WHERE date = ? ORDER BY symbol, ts",
                    [d],
                ).df()
                if df.empty:
                    continue
            df.to_parquet(out_path, index=False)
            print(f"[OUT] {d} -> {out_path} rows={len(df)}")
        return date_list
    finally:
        con.close()


def upload_out_to_s3(layout: KisDataLayout, date_list: List[str], freq: str) -> List[str]:
    s3 = boto3.client("s3")
    success: List[str] = []
    for d in date_list:
        local_path = layout.local_out_1d_date(d) if freq == "1d" else layout.local_out_1m_date(d)
        if not os.path.exists(local_path):
            print(f"[S3] file missing: {local_path}")
            continue
        s3_url = layout.s3_1d_date(d) if freq == "1d" else layout.s3_1m_date(d)
        bucket, key = _s3_split(s3_url)
        try:
            s3.upload_file(local_path, bucket, key)
            print(f"[S3] uploaded {local_path} -> {s3_url}")
            success.append(d)
        except Exception as e:
            print(f"[S3] upload failed {local_path}: {e}")
    return success


def cleanup_local(layout: KisDataLayout, date_list: List[str], freq: str) -> None:
    # OUT 정리
    for d in date_list:
        out_path = layout.local_out_1d_date(d) if freq == "1d" else layout.local_out_1m_date(d)
        if os.path.exists(out_path):
            os.remove(out_path)
        date_dir = os.path.dirname(out_path)
        if os.path.exists(date_dir) and not os.listdir(date_dir):
            os.rmdir(date_dir)

    # TEMP 정리
    temp_dir = layout.local_temp_by_symbol_1d() if freq == "1d" else layout.local_temp_by_symbol_1m()
    files = _list_temp_parquet_files(temp_dir)
    for f in files:
        os.remove(f)
    if os.path.exists(temp_dir) and not os.listdir(temp_dir):
        os.rmdir(temp_dir)


def run_compact_upload_cleanup(
    config_path: str,
    freq: str = "1d",
    mode: str = "incremental",
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> None:
    layout = load_kis_data_layout(config_path)
    date_list = compact_temp_to_out(layout, freq)
    if not date_list:
        return

    date_list = sorted(date_list)
    if mode == "backfill":
        if not start or not end:
            raise ValueError("backfill 모드에서는 --start와 --end가 필요합니다.")
        date_list = [d for d in date_list if start <= d <= end]
        print(f"[backfill] range={start}~{end}, target_dates={len(date_list)}")
    elif mode != "incremental":
        raise ValueError(f"unknown mode: {mode}")

    if not date_list:
        print("[pipeline] 대상 날짜가 없습니다.")
        return

    success = upload_out_to_s3(layout, date_list, freq)
    if len(success) != len(date_list):
        print(f"[S3] 일부 업로드 실패 ({len(success)}/{len(date_list)}) -> 정리 생략")
        return

    cleanup_local(layout, date_list, freq)
    print("[DONE] TEMP/OUT 정리 완료")
    send_telegram(
        f"[KIS][upload] S3 upload complete freq={freq} dates={date_list[0]}~{date_list[-1]} count={len(date_list)}"
    )
    ck_name = "market_data_1d_latest" if freq == "1d" else "market_data_1m_latest"
    checkpoints = load_checkpoints(config_path)
    latest_cp = checkpoints.get(ck_name)
    max_date = max(date_list)
    if not latest_cp or max_date > latest_cp:
        _update_checkpoint_config(config_path, ck_name, max_date)
    else:
        print(f"[checkpoint] keep: {ck_name}={latest_cp} (max_uploaded={max_date})")


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser(description="KIS TEMP -> OUT -> S3 파이프라인")
    ap.add_argument("--config", default="config.json", help="설정 파일 경로")
    ap.add_argument("--freq", default="1d", choices=["1d", "1m"])
    ap.add_argument("--mode", default="incremental", choices=["incremental", "backfill"])
    ap.add_argument("--start", default=None, help="YYYY-MM-DD (backfill)")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD (backfill)")
    args = ap.parse_args()

    config_path = args.config
    if not os.path.isabs(config_path) and not os.path.exists(config_path):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, config_path)
    
    run_compact_upload_cleanup(
        config_path,
            freq=args.freq,
        mode=args.mode,
        start=args.start,
        end=args.end,
    )


if __name__ == "__main__":
        main()
