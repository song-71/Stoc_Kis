"""
설정 파일 관리 유틸리티
"""
import json
import logging
import os
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, Optional, Sequence, Tuple

import hashlib
import pandas as pd
import polars as pl
import requests

# 국내휴장일조회 API (국내주식-040)
_API_PATH_HOLIDAY = "/uapi/domestic-stock/v1/quotations/chk-holiday"
_TR_ID_HOLIDAY = "CTCA0903R"
_KST = timezone(timedelta(hours=9))
_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")


def _is_open_day_via_api(target_dt: datetime) -> bool:
    """국내휴장일조회 API 호출로 개장일(Y) 여부 판단. kis_API_ohlcv_download_Utils는 lazy import로 순환 참조 방지."""
    from kis_API_ohlcv_download_Utils import DEFAULT_BASE_URL, KisClient, KisConfig

    cfg = load_config(_CONFIG_PATH)
    appkey = cfg.get("appkey")
    appsecret = cfg.get("appsecret")
    if not appkey or not appsecret:
        raise RuntimeError("config에 appkey/appsecret이 없습니다.")

    base_dir = os.path.dirname(_CONFIG_PATH)
    token_path = os.path.join(base_dir, "kis_token_main.json") if base_dir else "kis_token_main.json"

    client = KisClient(
        KisConfig(
            appkey=appkey,
            appsecret=appsecret,
            base_url=cfg.get("base_url") or DEFAULT_BASE_URL,
            custtype=cfg.get("custtype") or "P",
            market_div=cfg.get("market_div") or "J",
            token_cache_path=token_path,
        )
    )
    client.ensure_token()

    yyyymmdd = target_dt.strftime("%Y%m%d")
    url = f"{client.cfg.base_url}{_API_PATH_HOLIDAY}"
    headers = client._headers(tr_id=_TR_ID_HOLIDAY)
    params = {"BASS_DT": yyyymmdd, "CTX_AREA_NK": "", "CTX_AREA_FK": ""}

    r = requests.get(url, headers=headers, params=params, timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        raise RuntimeError(f"API오류: {j.get('msg1', '')}")

    output = j.get("output") or []
    if isinstance(output, dict):
        output = [output]
    row = output[0] if output else {}
    opnd_yn = str(row.get("opnd_yn", "N")).upper()
    return opnd_yn == "Y"


def is_holiday() -> bool:
    """
    오늘이 휴장일인지 반환합니다.

    - config의 today_open_chk: {date: "YYYY-MM-DD", status: "open"|"holiday"}
    - date가 오늘과 다르면 API 호출 후 결과를 config에 저장하고 반환
    - date가 오늘이면 저장된 status로 판단

    Returns:
        True: 휴장일, False: 개장일
    """
    cfg = load_config(_CONFIG_PATH)
    today = datetime.now(_KST).strftime("%Y-%m-%d")

    chk = cfg.get("today_open_chk")
    if isinstance(chk, dict):
        stored_date = str(chk.get("date", ""))
        stored_status = str(chk.get("status", "")).lower()
    else:
        stored_date = ""
        stored_status = ""

    if stored_date == today and stored_status in ("open", "holiday"):
        return stored_status == "holiday"

    # API 호출 후 config 저장
    try:
        target_dt = datetime.now(_KST)
        is_open = _is_open_day_via_api(target_dt)
    except Exception:
        raise

    status = "open" if is_open else "holiday"
    cfg["today_open_chk"] = {"date": today, "status": status}
    save_config(cfg, _CONFIG_PATH)
    return not is_open


def load_config(config_path: str = "config.json", account_id: str | None = None) -> Dict[str, str]:
    """
    설정 파일을 읽어서 딕셔너리로 반환합니다.
    
    Args:
        config_path: 설정 파일 경로 (기본값: config.json)
        account_id: 계정 ID (accounts 하위에서 오버라이드)
        
    Returns:
        설정 값들이 담긴 딕셔너리
        
    Raises:
        FileNotFoundError: 설정 파일이 존재하지 않을 때
        json.JSONDecodeError: JSON 파싱 오류
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"설정 파일을 찾을 수 없습니다: {config_path}")
    
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    
    if account_id:
        accounts = config.get("accounts", {})
        if isinstance(accounts, dict) and account_id in accounts:
            base_cfg = {k: v for k, v in config.items() if k != "accounts"}
            merged = dict(base_cfg)
            merged.update(accounts.get(account_id, {}))
            if "cano" in merged:
                merged.setdefault("cano1", merged.get("cano", ""))
                merged.setdefault("cano2", merged.get("cano", ""))
            return merged
    return config


def resolve_account_config(config_path: str = "config.json", account_id: str | None = None) -> Dict[str, str]:
    """
    공통 설정 + 계정별 오버라이드를 병합한 설정을 반환합니다.
    """
    return load_config(config_path=config_path, account_id=account_id)


def stQueryDB(code: str, parquet_path: str | None = None) -> Dict[str, Any]:
    """
    1d 파케이트 DB에서 종목코드 기준 최신 행을 반환합니다.
    반환 키 예: open, high, low, close, volume, value, pdy_close, R_avr 등
    """
    if not code:
        return {}
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = parquet_path or os.path.join(base_dir, "data", "1d_data", "kis_1d_unified_parquet_DB.parquet")
    if not os.path.exists(path):
        return {}
    cols = [
        "date",
        "symbol",
        "code",
        "name",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "pdy_close",
        "R_avr",
    ]
    try:
        df = pl.read_parquet(path, columns=cols)
    except Exception:
        try:
            df = pl.read_parquet(path)
        except Exception:
            return {}
    if df.is_empty():
        return {}
    if "symbol" in df.columns:
        df = df.filter(pl.col("symbol").cast(pl.Utf8) == str(code))
    elif "code" in df.columns:
        df = df.filter(pl.col("code").cast(pl.Utf8) == str(code))
    if df.is_empty():
        return {}
    if "date" in df.columns:
        try:
            df = df.sort("date")
        except Exception:
            pass
    row = df.tail(1).to_dicts()[0]
    return row


def now_kst_str() -> str:
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst).strftime("%y%m%d_%H%M%S")


class KSTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        kst = timezone(timedelta(hours=9))
        dt = datetime.fromtimestamp(record.created, tz=kst)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()


class _DropNoConsoleFilter(logging.Filter):
    """no_console=True 레코드는 콘솔 핸들러에서 제외 (중복 방지)"""

    def filter(self, record: logging.LogRecord) -> bool:
        return not getattr(record, "no_console", False)


class LogManager:
    """
    출력 3단계 통합 로거 (화면 출력 항상 보장, 텔레그램 전송 시 반드시 화면 선행)

    1단계 log_screen : 화면만
    2단계 log       : 화면 + 로그파일
    3단계 log_tm    : 화면 + 로그 + 텔레그램

    사용법:
        lm = LogManager(log_dir)
        lm.log_screen("화면만")
        lm.log("화면+로그")
        lm.log_tm("화면+로그+텔레그램")

    prefix_callback: 시간 접두어 함수 (예: lambda: "[260215_012345]") - 미지정 시 KST 현재시각 사용
    """

    def __init__(
        self,
        log_dir: str,
        log_name: str | None = None,
        run_tag: str | None = None,
        prefix_callback: Callable[[], str] | None = None,
    ):
        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)

        if log_name:
            log_file = os.path.join(self.log_dir, log_name)
        else:
            timestamp = now_kst_str()
            log_file = os.path.join(self.log_dir, f"KIS_log_{timestamp}.log")
        self.log_file = log_file

        kst = timezone(timedelta(hours=9))

        def _default_prefix() -> str:
            return "[" + datetime.now(kst).strftime("%y%m%d_%H:%M:%S") + "]"

        self._prefix = prefix_callback if prefix_callback else _default_prefix

        self.logger = logging.getLogger("KISLogger")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        if not self.logger.handlers:
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            ch.addFilter(_DropNoConsoleFilter())

            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setLevel(logging.INFO)

            formatter = KSTFormatter(
                "[%(asctime)s] %(message)s",
                datefmt="%y%m%d_%H:%M:%S",
            )
            ch.setFormatter(formatter)
            fh.setFormatter(formatter)

            self.logger.addHandler(ch)
            self.logger.addHandler(fh)

        if run_tag:
            try:
                with open(log_file, "a", encoding="utf-8") as f:
                    f.write("\n" + "=" * 20 + "\n")
                    f.write(run_tag + "\n")
            except Exception:
                pass
        print(f"📝 로그 파일: {log_file}")

    def _fmt(self, msg: str) -> str:
        """접두어 + 메시지 (log_screen, log_tm 화면 출력용)"""
        return self._prefix() + " " + msg

    def log_screen(self, msg: str) -> None:
        """1단계: 화면만 출력"""
        out = self._fmt(msg)
        sys.stdout.write(out + "\n")
        sys.stdout.flush()

    def log(self, msg: str) -> None:
        """2단계: 화면 + 로그 파일"""
        self.logger.info(msg)

    def log_tm(self, msg: str) -> None:
        """3단계: 화면 + 로그 + 텔레그램 (텔레그램 전송 시 반드시 화면 선행)"""
        out = self._fmt(msg)
        sys.stdout.write(out + "\n")
        sys.stdout.flush()
        self.logger.info(msg, extra={"no_console": True})
        try:
            from telegMsg import tmsg

            tmsg(out, "-t")
        except ImportError:
            pass
        except Exception as e:
            self.logger.warning(f"텔레그램 메시지 전송 실패: {e}")

    def log_raw(self, msg: str) -> None:
        """2단계(raw): 화면 + 로그, msg 그대로 (호출자가 접두어 포함하여 전달, 파일은 msg 그대로 기록)"""
        sys.stdout.write(msg + "\n")
        sys.stdout.flush()
        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except Exception:
            pass

    def log_file_only(self, msg: str) -> None:
        """파일만 기록 (화면 출력 없음)"""
        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except Exception:
            pass

    def log_tm_raw(self, msg: str) -> None:
        """3단계(raw): 화면 + 로그 + 텔레그램, msg 그대로 (호출자가 접두어 포함하여 전달)"""
        sys.stdout.write(msg + "\n")
        sys.stdout.flush()
        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except Exception:
            pass
        try:
            from telegMsg import tmsg

            tmsg(msg, "-t")
        except ImportError:
            pass
        except Exception as e:
            self.logger.warning(f"텔레그램 메시지 전송 실패: {e}")


class TeeStdout:
    def __init__(self, log_file: str):
        self.log_file = log_file
        self._file = open(log_file, "a", encoding="utf-8")
        self._stdout = sys.stdout

    def write(self, data: str) -> None:
        if data:
            self._file.write(data)
            self._file.flush()
        self._stdout.write(data)
        self._stdout.flush()

    def flush(self) -> None:
        self._file.flush()
        self._stdout.flush()

    def close(self) -> None:
        try:
            self._file.close()
        except Exception:
            pass


def display_width(text: str) -> int:
    width = 0
    for ch in text:
        if unicodedata.east_asian_width(ch) in ("W", "F"):
            width += 2
        else:
            width += 1
    return width


def pad_text(text: str, width: int, align: str = "left") -> str:
    text_width = display_width(text)
    if text_width >= width:
        return text
    pad = width - text_width
    if align == "right":
        return " " * pad + text
    if align == "center":
        left = pad // 2
        right = pad - left
        return " " * left + text + " " * right
    return text + " " * pad


def print_table(rows: list[dict], columns: list[str], align: dict[str, str],
                max_rows: int | None = None) -> None:
    """
    한글 폭 보정 정렬 테이블 출력.
      max_rows=None : 전체 행 출력
      max_rows=20   : 앞 10행 + ... + 뒤 10행 (중간 생략)
    """
    if not rows:
        return
    str_rows = []
    widths = {col: display_width(col) for col in columns}
    for row in rows:
        srow = {}
        for col in columns:
            val = row.get(col, "")
            s = "" if val is None else str(val)
            srow[col] = s
            widths[col] = max(widths[col], display_width(s))
        str_rows.append(srow)
    # 출력 대상 행 결정
    n = len(str_rows)
    if max_rows is not None and n > max_rows:
        half = max_rows // 2
        show_rows = str_rows[:half] + [None] + str_rows[-half:]
    else:
        show_rows = str_rows
    # 헤더 + 구분선
    header = "  ".join(pad_text(col, widths[col], "left") for col in columns)
    print(header)
    print("  ".join("-" * widths[col] for col in columns))
    # 행 출력
    for srow in show_rows:
        if srow is None:
            print("  ".join(pad_text("...", widths[col], "center") for col in columns))
        else:
            print("  ".join(pad_text(srow[col], widths[col], align.get(col, "left")) for col in columns))
    print(f"[{n} rows x {len(columns)} columns]")


def save_config(config: Dict[str, str], config_path: str = "config.json") -> None:
    """
    설정을 파일에 저장합니다.
    
    Args:
        config: 저장할 설정 딕셔너리
        config_path: 설정 파일 경로 (기본값: config.json)
    """
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


def get_config_value(config: Dict[str, str], key: str, default: Optional[str] = None) -> Optional[str]:
    """
    설정 딕셔너리에서 값을 가져옵니다.
    
    Args:
        config: 설정 딕셔너리
        key: 가져올 키
        default: 기본값 (키가 없을 때)
        
    Returns:
        설정 값 또는 기본값
    """
    return config.get(key, default)


def load_checkpoints(config_path: str = "config.json") -> Dict[str, str]:
    try:
        cfg = load_config(config_path)
    except FileNotFoundError:
        return {}
    return cfg.get("checkpoints", {}) or {}


def update_checkpoint(config_path: str, name: str, date_str: str) -> None:
    cfg = load_config(config_path)
    checkpoints = cfg.get("checkpoints", {}) or {}
    checkpoints[name] = date_str
    cfg["checkpoints"] = checkpoints
    save_config(cfg, config_path)


# =============================================================================
# KIS Data Layout (local + S3) + DuckDB helpers
# =============================================================================


def _default_local_root() -> str:
    return os.path.join(os.path.expanduser("~"), "Stoc_Kis", "data")


@dataclass(frozen=True)
class KisDataLayout:
    """
    표준 저장 구조 정의
    - 로컬: /home/ubuntu/Stoc_Kis/data (TEMP -> OUT -> S3)
    - S3: s3://tfttrain/KIS_DB/...
    """

    local_root: str = _default_local_root()
    s3_bucket: str = "tfttrain"
    s3_prefix: str = "KIS_DB"
    bucket_count_1m: int = 16

    # Local subpaths
    def local_temp_by_symbol_1d(self) -> str:
        return os.path.join(self.local_root, "temp", "by_symbol", "1d")

    def local_temp_by_symbol_1m(self) -> str:
        return os.path.join(self.local_root, "temp", "by_symbol", "1m")

    def local_out_1d_date(self, date_str: str) -> str:
        return os.path.join(
            self.local_root, "temp_out", "market_data", "1d", f"date={date_str}", "ohlcv.parquet"
        )

    def local_out_1m_date(self, date_str: str) -> str:
        return os.path.join(
            self.local_root, "temp_out", "market_data", "1m", f"date={date_str}", "ohlcv.parquet"
        )

    def local_out_1m_bucket(self, date_str: str, bucket: str) -> str:
        return os.path.join(
            self.local_root,
            "temp_out",
            "market_data",
            "1m",
            f"date={date_str}",
            f"bucket={bucket}",
            "ohlcv.parquet",
        )

    def local_features_1d_signal(self, date_str: str) -> str:
        return os.path.join(
            self.local_root, "temp_out", "features", "1d_signal", f"date={date_str}", "signal.parquet"
        )

    # S3 subpaths
    def s3_1d_date(self, date_str: str) -> str:
        return f"s3://{self.s3_bucket}/{self.s3_prefix}/market_data/1d/date={date_str}/ohlcv.parquet"

    def s3_1m_date(self, date_str: str) -> str:
        return f"s3://{self.s3_bucket}/{self.s3_prefix}/market_data/1m/date={date_str}/ohlcv.parquet"

    def s3_1m_bucket(self, date_str: str, bucket: str) -> str:
        return (
            f"s3://{self.s3_bucket}/{self.s3_prefix}/market_data/1m/"
            f"date={date_str}/bucket={bucket}/ohlcv.parquet"
        )

    def s3_features_1d_signal(self, date_str: str) -> str:
        return (
            f"s3://{self.s3_bucket}/{self.s3_prefix}/features/1d_signal/"
            f"date={date_str}/signal.parquet"
        )

    def s3_glob_1d(self) -> str:
        return f"s3://{self.s3_bucket}/{self.s3_prefix}/market_data/1d/date=*/ohlcv.parquet"

    def s3_glob_1m(self) -> str:
        return f"s3://{self.s3_bucket}/{self.s3_prefix}/market_data/1m/date=*/ohlcv.parquet"


def load_kis_data_layout(config_path: str = "config.json") -> KisDataLayout:
    """
    config.json에서 저장 구조 기본값을 읽어 KisDataLayout 반환
    (없으면 기본값 사용)
    """
    cfg: Dict[str, str] = {}
    try:
        cfg = load_config(config_path)
    except FileNotFoundError:
        cfg = {}

    return KisDataLayout(
        local_root=cfg.get("local_root", _default_local_root()),
        s3_bucket=cfg.get("s3_bucket", "tfttrain"),
        s3_prefix=cfg.get("s3_prefix", "KIS_DB"),
        bucket_count_1m=int(cfg.get("bucket_count_1m", 16)),
    )


def _tm(msg: str) -> None:
    """
    텔레그램 메시지 전송
    - telegMsg.tmsg(msg, "-t") 사용 (TFT_utils 방식)
    """
    try:
        from telegMsg import tmsg

        tmsg(msg, "-t")
        
    except Exception:
        return


def send_telegram(msg: str) -> None:
    _tm(msg)


def symbol_master_path(layout: KisDataLayout) -> str:
    return os.path.join(layout.local_root, "admin", "symbol_master", "KRX_code.parquet")


def load_symbol_master(config_path: str = "config.json"):
    import pandas as pd

    layout = load_kis_data_layout(config_path)
    path = symbol_master_path(layout)
    if not os.path.exists(path):
        raise FileNotFoundError(f"symbol master parquet 없음: {path}")
    return pd.read_parquet(path)


_SYMBOL_MASTER_CACHE = {}


def load_symbol_master_cached(config_path: str = "config.json"):
    if config_path in _SYMBOL_MASTER_CACHE:
        return _SYMBOL_MASTER_CACHE[config_path]
    df = load_symbol_master(config_path)
    _SYMBOL_MASTER_CACHE[config_path] = df
    return df


def _normalize_symbol_code(text: str) -> str:
    return str(text).strip().zfill(6)


def KRX_code(
    query: str, config_path: str = "config.json"
) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    """
    입력된 종목코드/명에 대해 code, name, nxt, market, iscd_stat_cls_code(종목상태) 반환
      * 종목코드/명 : 종목코드(6자리 미만 포함) 또는 종목명(한글 포함)
      * 영문은 소문자도 검색 허용
      * iscd_stat_cls_code: 57/59=30분 단일가, 58=상장폐지 예정(거래 제외) 등
    """
    q = str(query).strip()
    if not q:
        return None, None, None, None, None
    df = load_symbol_master_cached(config_path)
    if "code" not in df.columns or "name" not in df.columns:
        return None, None, None, None, None

    def _market_from_row(row) -> str | None:
        if "market" not in row.index:
            return None
        v = row["market"]
        if pd.isna(v) or v == "" or str(v) in ("nan", "None"):
            return None
        return str(v)

    def _status_from_row(row) -> str | None:
        if "iscd_stat_cls_code" not in row.index:
            return None
        v = row["iscd_stat_cls_code"]
        if pd.isna(v) or v == "" or str(v) in ("nan", "None"):
            return None
        return str(v)

    # code match
    code = _normalize_symbol_code(q)
    exact = df[df["code"].astype(str).map(_normalize_symbol_code) == code]
    if not exact.empty:
        row = exact.iloc[0]
        nxt = str(row["nxt"]) if "nxt" in row.index else "N"
        return code, str(row["name"]), nxt, _market_from_row(row), _status_from_row(row)
    # name match (영문은 대소문자 구분 없이 검색: q.upper()와 비교)
    match = df[df["name"].astype(str).str.upper() == q.upper()]
    if not match.empty:
        row = match.iloc[0]
        code_val = _normalize_symbol_code(row["code"])
        nxt = str(row["nxt"]) if "nxt" in row.index else "N"
        return code_val, str(row["name"]), nxt, _market_from_row(row), _status_from_row(row)
    return None, None, None, None, None


def KRX_code_batch(
    symbols: list[str], config_path: str = "config.json"
) -> dict[str, str | None]:
    """
    여러 종목코드에 대해 iscd_stat_cls_code(종목상태) 반환. code -> status 매핑.
    KRX_code와 동일 데이터 소스 사용. merge로 일괄 조회하여 성능 최적화.
    """
    if not symbols:
        return {}
    df = load_symbol_master_cached(config_path)
    if "code" not in df.columns or "iscd_stat_cls_code" not in df.columns:
        return {s: None for s in symbols}
    lookup = pd.DataFrame(
        {"_q": symbols, "_norm": [_normalize_symbol_code(s) for s in symbols]}
    )
    master = df[["code", "iscd_stat_cls_code"]].copy()
    master["_norm"] = master["code"].astype(str).map(_normalize_symbol_code)
    master = master.drop_duplicates(subset=["_norm"], keep="first")
    merged = lookup.merge(master[["_norm", "iscd_stat_cls_code"]], on="_norm", how="left")
    result: dict[str, str | None] = {}
    for orig, val in zip(merged["_q"], merged["iscd_stat_cls_code"]):
        if pd.isna(val) or str(val) in ("", "nan", "None"):
            result[orig] = None
        else:
            result[orig] = str(val)
    return result


def resolve_symbol(query: str, df):
    q = query.strip()
    if not q:
        return None, []
    # code exact match
    exact = df[df["code"] == q]
    if not exact.empty:
        row = exact.iloc[0]
        return row, []
    # name contains
    matches = df[df["name"].str.contains(q, na=False)]
    if len(matches) == 1:
        return matches.iloc[0], []
    return None, matches


def bucket_for_symbol(symbol: str, bucket_count: int = 16) -> str:
    """
    symbol을 안정적으로 버킷 분배 (00 ~ N-1)
    - hash(symbol) % N (SHA1 사용)
    """
    h = hashlib.sha1(symbol.encode("utf-8")).hexdigest()
    idx = int(h, 16) % bucket_count
    return f"{idx:02d}"


def s3_paths_1m_for_symbols(
    layout: KisDataLayout,
    date_list: Sequence[str],
    symbols: Sequence[str],
) -> Sequence[str]:
    """
    1m 조회용 S3 경로 리스트 (date + bucket 제한)
    """
    buckets = {bucket_for_symbol(sym, layout.bucket_count_1m) for sym in symbols}
    paths = []
    for d in date_list:
        for b in sorted(buckets):
            paths.append(layout.s3_1m_bucket(d, b))
    return paths


def list_s3_1d_paths(layout: KisDataLayout, date_from: str, date_to: str) -> list[str]:
    import boto3

    s3 = boto3.client("s3")
    prefix = f"{layout.s3_prefix}/market_data/1d/date="
    paginator = s3.get_paginator("list_objects_v2")
    paths: list[str] = []
    for page in paginator.paginate(Bucket=layout.s3_bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            key = obj.get("Key", "")
            if not key.endswith("/ohlcv.parquet"):
                continue
            try:
                part = key.split("date=", 1)[1]
                date_str = part.split("/", 1)[0]
            except Exception:
                continue
            if date_from <= date_str <= date_to:
                paths.append(f"s3://{layout.s3_bucket}/{key}")
    return sorted(paths)


def _kr_tick_size(price: float) -> int:
    if price < 1000:
        return 1
    if price < 5000:
        return 5
    if price < 10000:
        return 10
    if price < 50000:
        return 50
    if price < 100000:
        return 100
    if price < 500000:
        return 500
    return 1000


def calc_limit_up_price(prev_close: float) -> float:
    if prev_close is None:
        return float("nan")
    try:
        if prev_close != prev_close:
            return float("nan")
    except Exception:
        return float("nan")
    raw = prev_close * 1.3
    tick = _kr_tick_size(raw)
    return round(raw / tick) * tick


def _configure_duckdb_s3(con, config_path: str = "config.json") -> None:
    def _imds_token() -> Optional[str]:
        try:
            import requests

            r = requests.put(
                "http://169.254.169.254/latest/api/token",
                headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
                timeout=1,
            )
            if r.status_code == 200:
                return r.text
        except Exception:
            return None
        return None

    def _imds_region() -> Optional[str]:
        try:
            import requests

            token = _imds_token()
            headers = {"X-aws-ec2-metadata-token": token} if token else {}
            r = requests.get(
                "http://169.254.169.254/latest/dynamic/instance-identity/document",
                headers=headers,
                timeout=1,
            )
            if r.status_code == 200:
                data = r.json()
                return data.get("region")
        except Exception:
            return None
        return None

    try:
        con.execute("INSTALL httpfs;")
    except Exception:
        pass
    con.execute("LOAD httpfs;")
    cfg: Dict[str, str] = {}
    try:
        cfg = load_config(config_path)
    except FileNotFoundError:
        cfg = {}
    region = (
        cfg.get("s3_region")
        or os.getenv("AWS_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
        or _imds_region()
    )
    access_key = cfg.get("aws_access_key_id") or os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = cfg.get("aws_secret_access_key") or os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = cfg.get("aws_session_token") or os.getenv("AWS_SESSION_TOKEN")
    endpoint = cfg.get("s3_endpoint") or os.getenv("AWS_S3_ENDPOINT")
    try:
        con.execute("SET s3_use_instance_profile=true;")
    except Exception:
        pass
    if region:
        con.execute(f"SET s3_region='{region}';")
    if not access_key or not secret_key:
        try:
            import boto3

            creds = boto3.Session().get_credentials()
            if creds:
                frozen = creds.get_frozen_credentials()
                access_key = access_key or frozen.access_key
                secret_key = secret_key or frozen.secret_key
                session_token = session_token or frozen.token
        except Exception:
            pass

    if access_key and secret_key:
        con.execute(f"SET s3_access_key_id='{access_key}';")
        con.execute(f"SET s3_secret_access_key='{secret_key}';")
    if session_token:
        con.execute(f"SET s3_session_token='{session_token}';")
    if endpoint:
        con.execute(f"SET s3_endpoint='{endpoint}';")


def duckdb_read_1d_s3(
    layout: KisDataLayout,
    date_from: str,
    date_to: str,
    symbols: Sequence[str],
) -> "object":
    """
    DuckDB로 S3 1d 데이터 조회 (date 파티션)
    """
    import duckdb
    import pandas as pd

    paths = list_s3_1d_paths(layout, date_from, date_to)
    if not paths:
        cols = ["date", "symbol", "open", "high", "low", "close", "volume", "value"]
        return duckdb.from_df(pd.DataFrame(columns=cols)).df()

    symbols_sql = ",".join([f"'{s}'" for s in symbols])
    files_sql = "[" + ",".join([f"'{p}'" for p in paths]) + "]"
    query = f"""
    SELECT date, symbol, open, high, low, close, volume, value
    FROM read_parquet({files_sql})
    WHERE date BETWEEN '{date_from}' AND '{date_to}'
      AND symbol IN ({symbols_sql})
    ORDER BY symbol, date
    """
    con = duckdb.connect()
    try:
        _configure_duckdb_s3(con)
        return con.execute(query).df()
    finally:
        con.close()


def duckdb_read_1m_s3(
    layout: KisDataLayout,
    date_list: Sequence[str],
    symbols: Sequence[str],
) -> "object":
    """
    DuckDB로 S3 1m 데이터 조회 (date + bucket 제한)
    """
    import duckdb

    symbols_sql = ",".join([f"'{s}'" for s in symbols])
    date_sql = ",".join([f"'{d}'" for d in date_list])
    query = f"""
    SELECT date, ts, symbol, open, high, low, close, volume, value
    FROM read_parquet('{layout.s3_glob_1m()}', hive_partitioning=1)
    WHERE date IN ({date_sql})
      AND symbol IN ({symbols_sql})
    ORDER BY symbol, ts
    """
    con = duckdb.connect()
    try:
        _configure_duckdb_s3(con)
        return con.execute(query).df()
    finally:
        con.close()
