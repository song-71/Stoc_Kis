
PRINT_OPTION = 1  # 1: 원본 저장 메시지, 2: 세션 요약 메시지

# 종목명/코드 혼용 입력 (직접 수정)
""" 26.2.4. 수신대상 종목(업종) 목록 """
CODE_TEXT = """
삼표시멘트
전진건설로봇
이녹스
미래에셋벤처투자
이미지스
코이즈
제이에스링크
다산디엠씨
하이즈항공
LK삼양
네오셈
HD건설기계
한화시스템
셀레믹스
유디엠텍
파두

"""

"""
백그라운드 실행:
nohup /home/ubuntu/Stoc_Kis/venv/bin/python /home/ubuntu/Stoc_Kis/ws_trading_data_collect.py > /home/ubuntu/Stoc_Kis/data/logs/ws_collect_nohup.log 2>&1 &

로그 보기:
tail -f /home/ubuntu/Stoc_Kis/data/logs/ws_collect_nohup.log
tail -f /home/ubuntu/Stoc_Kis/data/logs/wss_collect_YYYYMMDD.log

PID 확인:
pgrep -f ws_trading_data_collect.py

종료:
kill <PID>

PID 확인 및 종료(동시)
  pgrep -f ws_trading_data_collect.py | xargs kill


종료 확인:
ps -p <PID>
"""


import logging
import queue
import sys
import threading
import multiprocessing as mp
import time
from datetime import datetime, time as dt_time
from pathlib import Path
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# open-trading-api 예제 경로 추가 (domestic_stock_functions_ws.py 위치)
OPEN_API_WS_DIR = Path.home() / "open-trading-api" / "examples_user" / "domestic_stock"
sys.path.append(str(OPEN_API_WS_DIR))

# kis_auth_llm을 kis_auth로 치환하여 예제 모듈과 호환
import kis_auth_llm as ka  # noqa: E402

sys.modules["kis_auth"] = ka

from domestic_stock_functions_ws import ccnl_krx, exp_ccnl_krx  # noqa: E402
from kis_utils import load_symbol_master, send_telegram  # noqa: E402


DATA_DIR = Path("/home/ubuntu/Stoc_Kis/data/wss_data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR = Path("/home/ubuntu/Stoc_Kis/data/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
KST = ZoneInfo("Asia/Seoul")
OUTPUT_TAG = ""  # e.g. "KRX100" to include in filenames

WAIT_AFTER_TIME = dt_time(7, 0)
START_TIME = dt_time(8, 30)
REAL_START_TIME = dt_time(8, 59, 59)
EXP_PM_START_TIME = dt_time(15, 10)
REAL_PM_START_TIME = dt_time(16, 30)
EXP_AH_START_TIME = dt_time(16, 0)
MARKET_CLOSE_TIME = dt_time(15, 31)
RESUME_WS_TIME = dt_time(15, 58)
END_TIME = dt_time(18, 1)
MAX_SYMBOLS_PER_SESSION = 10
FLUSH_MAX_ROWS = 5000
FLUSH_INTERVAL_SEC = 5.0



OUTPUT_COLUMNS = [
    "isu_cd",
    "trd_tm",
    "trd_prc",
    "trd_vol",
    "acml_vol",
    "acml_tr_amt",
    "open_prc",
    "high_prc",
    "low_prc",
    "prdy_vrss",
    "prdy_ctrt",
    "trd_data",
]

logging.getLogger().setLevel(logging.ERROR)
logging.getLogger().disabled = True
_LOGGER = logging.getLogger("ws_collect")


class _FilteredStdout:
    def __init__(self, target):
        self._target = target
        self._buf = ""
        self._drop_tokens = ("PINGPONG", "received message >>", "Connection exception")

    def write(self, data: str) -> None:
        if not data:
            return
        self._buf += data
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            if any(tok in line for tok in self._drop_tokens):
                continue
            self._target.write(line + "\n")
            self._target.flush()
        if self._buf and self._buf.startswith("\r"):
            self._target.write(self._buf)
            self._target.flush()
            self._buf = ""

    def flush(self) -> None:
        self._target.flush()

    def isatty(self) -> bool:
        return self._target.isatty()


def _setup_logger(date_str: str) -> None:
    if _LOGGER.handlers:
        return
    try:
        cutoff = time.time() - (5 * 24 * 60 * 60)
        for path in LOG_DIR.glob("wss_collect_*.log"):
            try:
                if path.stat().st_mtime < cutoff:
                    path.unlink(missing_ok=True)
            except Exception:
                continue
    except Exception:
        pass
    log_path = LOG_DIR / f"wss_collect_{date_str}.log"
    log_fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(log_fmt)
    _LOGGER.setLevel(logging.INFO)
    _LOGGER.propagate = False
    _LOGGER.addHandler(file_handler)


def _now_kst() -> datetime:
    return datetime.now(KST)


def _fmt_ts(dt: datetime | None = None) -> str:
    return (dt or _now_kst()).strftime("%y%m%d_%H%M%S")


def _tag_prefix() -> str:
    return f"{OUTPUT_TAG}_" if OUTPUT_TAG else ""


def _session_file_name(date_str: str, session_id: int) -> str:
    return f"{date_str}_{_tag_prefix()}session{session_id}_wss_data.parquet"


def _final_file_name(date_str: str) -> str:
    return f"{date_str}_{_tag_prefix()}wss_data.parquet"


def _session_glob(date_str: str) -> str:
    return f"{date_str}_{_tag_prefix()}session*_wss_data.parquet"


def _chunked(items: List[str], size: int) -> List[List[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _parse_codes(text: str) -> List[str]:
    name_to_code = {}
    try:
        sdf = load_symbol_master()
        sdf["code"] = sdf["code"].astype(str).str.zfill(6)
        name_to_code = dict(zip(sdf["name"].astype(str), sdf["code"]))
    except Exception:
        name_to_code = {}
    codes: List[str] = []
    for line in text.splitlines():
        raw = line.strip()
        if not raw:
            continue
        if raw.isdigit():
            codes.append(raw.zfill(6))
            continue
        code = name_to_code.get(raw)
        if code:
            codes.append(code)
    return codes


def _normalize_ws_rows(df: pd.DataFrame, trd_flag: bool) -> List[Dict[str, Any]]:
    if df.empty:
        return []
    col_map = {str(c).lower(): c for c in df.columns}

    def _col(name: str) -> str | None:
        return col_map.get(name)

    fields = {
        "isu_cd": _col("mksc_shrn_iscd"),
        "trd_tm": _col("stck_cntg_hour"),
        "trd_prc": _col("stck_prpr"),
        "trd_vol": _col("cntg_vol"),
        "acml_vol": _col("acml_vol"),
        "acml_tr_amt": _col("acml_tr_pbmn"),
        "open_prc": _col("stck_oprc"),
        "high_prc": _col("stck_hgpr"),
        "low_prc": _col("stck_lwpr"),
        "prdy_vrss": _col("prdy_vrss"),
        "prdy_ctrt": _col("prdy_ctrt"),
    }

    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        out: Dict[str, Any] = {"trd_data": trd_flag}
        for key, col in fields.items():
            out[key] = row[col] if col else None
        rows.append(out)
    return rows


def _ws_worker(
    codes: List[str],
    out_queue: "mp.Queue[tuple[List[Dict[str, Any]], str]]",
    mode: str,
) -> None:
    sys.stdout = _FilteredStdout(sys.stdout)
    logging.getLogger().setLevel(logging.ERROR)
    logging.getLogger().disabled = True
    trd_flag = mode == "real"
    while True:
        try:
            ka.auth(svr="prod")
            ka.auth_ws(svr="prod")
            if not getattr(ka, "_base_headers_ws", {}).get("approval_key"):
                raise RuntimeError("auth_ws() 실패: kis_devlp.yaml의 앱키/시크릿을 확인하세요.")

            def on_result(ws, tr_id: str, result: pd.DataFrame, data_info: dict) -> None:
                rows = _normalize_ws_rows(result, trd_flag=trd_flag)
                if rows:
                    try:
                        out_queue.put((rows, mode))
                    except Exception:
                        pass

            kws = ka.KISWebSocket(api_url="")
            if mode == "real":
                kws.subscribe(request=ccnl_krx, data=codes)
            else:
                kws.subscribe(request=exp_ccnl_krx, data=codes)
            kws.start(on_result=on_result)
        except Exception as e:
            codes_preview = ", ".join(codes[:5]) + (f" +{len(codes)-5}" if len(codes) > 5 else "")
            ts = _fmt_ts()
            print(
                f"\n[{ts}][WS][{mode}] connection error: {e}; retry in 1s; codes={codes_preview}",
                flush=True,
            )
            time.sleep(1)


class SessionWriter:
    def __init__(
        self,
        session_id: int,
        date_str: str,
        total_codes: int,
        save_queue: "mp.Queue[Dict[str, Any]] | None",
    ):
        self.session_id = session_id
        self.date_str = date_str
        self.total_codes = total_codes
        self.save_queue = save_queue
        self.queue: "queue.Queue[List[Dict[str, Any]]]" = queue.Queue()
        self.stop_event = threading.Event()
        self.writer: pq.ParquetWriter | None = None
        self.session_path = DATA_DIR / _session_file_name(date_str, session_id)
        self.thread = threading.Thread(target=self._run, daemon=True)
        self._last_flush = time.time()
        self._seen_codes: set[str] = set()
        self._rate_ts = time.time()
        self._rate_count = 0
        self._last_rate = 0.0
        self._last_heartbeat = time.time()

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=10)
        if self.writer is not None:
            self.writer.close()
            self.writer = None

    def enqueue(self, rows: List[Dict[str, Any]]) -> None:
        if rows:
            self.queue.put(rows)

    def _flush(self, buffer_rows: List[Dict[str, Any]]) -> None:
        if not buffer_rows:
            return
        df = pd.DataFrame(buffer_rows)
        for col in OUTPUT_COLUMNS:
            if col not in df.columns:
                df[col] = None
        df = df[OUTPUT_COLUMNS]
        for col in OUTPUT_COLUMNS:
            if col == "trd_data":
                df[col] = df[col].astype(bool)
            else:
                df[col] = df[col].astype(str)
        codes_in_flush = set(df["isu_cd"].astype(str).tolist())
        self._seen_codes.update(codes_in_flush)
        table = pa.Table.from_pandas(df, preserve_index=False)
        if self.writer is None:
            self.writer = pq.ParquetWriter(self.session_path, table.schema)
        self.writer.write_table(table)
        if self.save_queue is not None:
            try:
                self.save_queue.put(
                    {
                        "ts": _fmt_ts(),
                        "session_id": self.session_id,
                        "rows": len(df),
                        "codes_in_flush": len(codes_in_flush),
                        "seen_codes": len(self._seen_codes),
                        "total_codes": self.total_codes,
                        "rate": self._last_rate,
                    }
                )
            except Exception:
                pass
        self._last_flush = time.time()

    def _run(self) -> None:
        buffer_rows: List[Dict[str, Any]] = []
        while not self.stop_event.is_set() or not self.queue.empty():
            try:
                rows = self.queue.get(timeout=0.5)
                buffer_rows.extend(rows)
                self._rate_count += len(rows)
            except queue.Empty:
                pass
            now = time.time()
            if now - self._rate_ts >= 1.0:
                elapsed = now - self._rate_ts
                self._last_rate = (self._rate_count / elapsed) if elapsed > 0 else 0.0
                self._rate_count = 0
                self._rate_ts = now
            if buffer_rows and (
                len(buffer_rows) >= FLUSH_MAX_ROWS
                or (now - self._last_flush) >= FLUSH_INTERVAL_SEC
            ):
                self._flush(buffer_rows)
                buffer_rows = []
            if not buffer_rows and (now - self._last_heartbeat) >= FLUSH_INTERVAL_SEC:
                self._last_heartbeat = now
        if buffer_rows:
            self._flush(buffer_rows)


def _wait_until_start() -> bool:
    now = _now_kst()
    if now.time() >= END_TIME:
        print(f"[{_fmt_ts(now)}] start skipped: now >= END_TIME", flush=True)
        return False
    if now.time() < WAIT_AFTER_TIME:
        print(f"[{_fmt_ts(now)}] start skipped: now < WAIT_AFTER_TIME", flush=True)
        return False
    if now.time() >= START_TIME:
        return True
    _log_status("07시 이후에는 08:30분까지 대기합니다.")
    while _now_kst().time() < START_TIME:
        time.sleep(5)
    return True


def _should_stop() -> bool:
    now = _now_kst()
    return now.time() >= END_TIME


def _log_status(msg: str) -> None:
    if sys.stdout.isatty():
        sys.stdout.write("\r\033[2K" + msg + "\n")
        sys.stdout.flush()
    _LOGGER.info(msg)


def _log_status_stdout(msg: str) -> None:
    if sys.stdout.isatty():
        sys.stdout.write("\r\033[2K" + msg + "\n")
        sys.stdout.flush()
    else:
        print(msg, flush=True)
    _LOGGER.info(msg)


class _StageTracker:
    def __init__(
        self,
        name: str,
        channel: str,
        start_time: dt_time,
        end_time: dt_time | None,
        start_msg: str,
        summary_after_sec: int,
        summary_msg: str,
        missing_msg: str | None = None,
        first_msg: str | None = None,
    ) -> None:
        self.name = name
        self.channel = channel
        self.start_time = start_time
        self.end_time = end_time
        self.start_msg = start_msg
        self.summary_after_sec = summary_after_sec
        self.summary_msg = summary_msg
        self.missing_msg = missing_msg
        self.first_msg = first_msg
        self.started_at: float | None = None
        self.seen_codes: set[str] = set()
        self.summary_done = False
        self.skipped = False

    def start(self, total_codes: int) -> None:
        if self.started_at is not None:
            return
        if self.skipped:
            return
        self.started_at = time.time()
        self.seen_codes.clear()
        _log_status(self.start_msg.format(total=total_codes))

    def handle_code(self, code: str, name: str, total_codes: int) -> None:
        if self.started_at is None:
            return
        if not code:
            return
        if code in self.seen_codes:
            return
        self.seen_codes.add(code)
        missing = total_codes - len(self.seen_codes)
        if self.first_msg is not None:
            _log_status(self.first_msg.format(name=name, missing=missing))

    def maybe_summary(self, total_codes: int) -> None:
        if self.started_at is None or self.summary_done:
            return
        if time.time() - self.started_at >= self.summary_after_sec:
            recv_cnt = len(self.seen_codes)
            _log_status(self.summary_msg.format(total=total_codes, recv=recv_cnt))
            if self.missing_msg is not None:
                missing = total_codes - recv_cnt
                _log_status(self.missing_msg.format(missing=missing))
            self.summary_done = True


def _run_session(
    session_id: int,
    codes: List[str],
    date_str: str,
    count_queue: "mp.Queue[int]",
    event_queue: "mp.Queue[tuple[str, str]]",
    save_queue: "mp.Queue[Dict[str, Any]]",
    total_codes: int,
) -> None:
    _setup_logger(date_str)
    sys.stdout = _FilteredStdout(sys.stdout)
    codes_set = set(codes)
    writer = SessionWriter(session_id, date_str, total_codes, save_queue)
    writer.start()
    ws_queue: "mp.Queue[tuple[List[Dict[str, Any]], str]]" = mp.Queue()
    real_proc: mp.Process | None = None
    exp_proc: mp.Process | None = None

    while not _should_stop():
        now_t = _now_kst().time()

        if real_proc is None:
            real_proc = mp.Process(
                target=_ws_worker, args=(codes, ws_queue, "real"), daemon=True
            )
            real_proc.start()
        if exp_proc is None:
            exp_proc = mp.Process(
                target=_ws_worker, args=(codes, ws_queue, "exp"), daemon=True
            )
            exp_proc.start()

        while True:
            try:
                rows, channel = ws_queue.get_nowait()
            except Exception:
                break
            writer.enqueue(rows)
            if rows:
                try:
                    count_queue.put(len(rows))
                except Exception:
                    pass
                df = pd.DataFrame(rows)
                if "isu_cd" in df.columns:
                    uniq_codes = set(df["isu_cd"].astype(str).tolist())
                    for code in uniq_codes:
                        if code in codes_set:
                            try:
                                event_queue.put((channel, code))
                            except Exception:
                                pass
        time.sleep(0.2)

    if real_proc is not None:
        real_proc.terminate()
    if exp_proc is not None:
        exp_proc.terminate()
    writer.stop()


def _merge_session_files(date_str: str) -> Path:
    final_path = DATA_DIR / _final_file_name(date_str)
    if final_path.exists():
        final_path.unlink()

    writer: pq.ParquetWriter | None = None
    total_rows = 0
    session_paths = sorted(DATA_DIR.glob(_session_glob(date_str)))
    backup_dir = DATA_DIR / "backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    for session_path in session_paths:
        if session_path.stat().st_size == 0:
            _log_status(f"[merge] 빈 파일 건너뜀 path={session_path}")
            session_path.unlink(missing_ok=True)
            continue
        try:
            table = pq.read_table(session_path)
        except Exception as exc:
            _log_status(f"[merge] 읽기 실패, 건너뜀 path={session_path} err={exc}")
            session_path.unlink(missing_ok=True)
            continue
        total_rows += table.num_rows
        if writer is None:
            writer = pq.ParquetWriter(final_path, table.schema)
        writer.write_table(table)
        backup_path = backup_dir / session_path.name
        try:
            session_path.replace(backup_path)
        except Exception:
            session_path.unlink(missing_ok=True)
    if writer is not None:
        writer.close()
    if final_path.exists() and total_rows > 0:
        size_mb = final_path.stat().st_size / (1024 * 1024)
        _log_status(
            f"[merge] 완료 path={final_path} rows={total_rows} size={size_mb:.2f}MB"
        )
    elif final_path.exists():
        final_path.unlink(missing_ok=True)
        _log_status("[merge] 병합 데이터가 없어 최종 파일을 저장하지 않았습니다.")
    return final_path


def main() -> None:
    start_ts = _fmt_ts()
    start_msg = f"[WSS][{start_ts}] ws_trading_data_collect.py 시작"
    print(start_msg, flush=True)
    send_telegram(start_msg)

    now = _now_kst()
    if now.time() >= END_TIME:
        date_str = now.strftime("%Y%m%d")
        _setup_logger(date_str)
        session_paths = sorted(DATA_DIR.glob(f"{date_str}_session*_wss_data.parquet"))
        if session_paths:
            msg_merge = "18:01 이후 실행: 기존 세션 파일을 병합합니다."
            _log_status_stdout(msg_merge)
            send_telegram(msg_merge)
            final_path = _merge_session_files(date_str)
            if final_path.exists():
                short_final = str(final_path).replace("/home/ubuntu/Stoc_Kis", "...", 1)
                _log_status_stdout(f"최종 저장경로: {short_final}")
                _log_status_stdout("최종 저장을 완료했습니다.")
            else:
                _log_status_stdout("병합 실패로 최종 저장을 하지 못했습니다.")
        end_msg = f"[WSS][{start_ts}] 정규 시간외로 종료합니다."
        print(end_msg, flush=True)
        send_telegram(end_msg)
        return

    if not _wait_until_start():
        end_ts = _fmt_ts()
        end_msg = f"[WSS][{start_ts}] 정규 시간외로 종료합니다."
        print(end_msg, flush=True)
        send_telegram(end_msg)
        return
    date_str = _now_kst().strftime("%Y%m%d")
    _setup_logger(date_str)
    codes = _parse_codes(CODE_TEXT)
    if not codes:
        no_code_msg = f"[WSS][{start_ts}] 종목코드 없음"
        print(no_code_msg, flush=True)
        send_telegram(no_code_msg)
        end_ts = _fmt_ts()
        end_msg = f"[WSS][{start_ts}] ws_trading_data_collect.py 종료"
        print(end_msg, flush=True)
        send_telegram(end_msg)
        return
    total_codes = len(codes)
    try:
        sdf = load_symbol_master()
        sdf["code"] = sdf["code"].astype(str).str.zfill(6)
        code_to_name = dict(zip(sdf["code"], sdf["name"].astype(str)))
    except Exception:
        code_to_name = {}

    total_codes = len(codes)
    sessions = _chunked(codes, MAX_SYMBOLS_PER_SESSION)
    procs: List[mp.Process] = []
    count_queue: "mp.Queue[int]" = mp.Queue()
    event_queue: "mp.Queue[tuple[str, str]]" = mp.Queue()
    save_queue: "mp.Queue[Dict[str, Any]]" = mp.Queue()
    stop_counter = threading.Event()

    def _rate_printer() -> None:
        last_ts = time.time()
        interval = 1.0 if sys.stdout.isatty() else 60.0
        count = 0
        while not stop_counter.is_set():
            try:
                count += count_queue.get(timeout=0.2)
            except Exception:
                pass
            now = time.time()
            if now - last_ts >= interval:
                ts = _fmt_ts()
                msg = f"[{ts}][{count:03d}건/초]"
                if sys.stdout.isatty():
                    sys.stdout.write("\r\033[2K" + msg)
                    sys.stdout.flush()
                last_ts = now
                count = 0
        print("")

    printer_thread = threading.Thread(target=_rate_printer, daemon=True)
    printer_thread.start()

    for i, batch in enumerate(sessions, start=1):
        p = mp.Process(
            target=_run_session,
            args=(i, batch, date_str, count_queue, event_queue, save_queue, total_codes),
        )
        p.start()
        procs.append(p)
        time.sleep(0.5)

    stages: List[_StageTracker] = [
        _StageTracker(
            name="exp_am",
            channel="exp",
            start_time=START_TIME,
            end_time=REAL_START_TIME,
            start_msg="08:30 : {total}개 종목에 대한 예상체결가 조회를 시작합니다.",
            summary_after_sec=60,
            summary_msg="1분후 총 {total}개 종목 중 {recv}개 종목의 wss데이터가 수신되고 있습니다.",
            missing_msg="미수신된 종목 {missing}건 수신을 대기합니다.",
            first_msg="{name}종목의 데이터가 수신되기 시작했습니다.(미결 {missing} 종목 대기 중..)",
        ),
        _StageTracker(
            name="real_am",
            channel="real",
            start_time=REAL_START_TIME,
            end_time=MARKET_CLOSE_TIME,
            start_msg="08:59:59: 정규장 시간이 되어 {total}개 종목에 대한 실시간 체결가 조회를 시작합니다.",
            summary_after_sec=60,
            summary_msg="1분 후 총 {total}개 종목 중 {recv}개 종목에 대한 데이터가 수신되고 있습니다.",
            missing_msg="미수신된 종목 {missing}건 수신을 대기합니다.",
            first_msg="{name}종목에 대한 수신이 시작되었습니다.",
        ),
        _StageTracker(
            name="exp_pm",
            channel="exp",
            start_time=EXP_PM_START_TIME,
            end_time=MARKET_CLOSE_TIME,
            start_msg="15:10분이 되면 : 예상체결가 데이터 수신을 시작합니다.",
            summary_after_sec=60,
            summary_msg="1분 후 총 {total}개 종목 중 {recv}개 종목에 대한 데이터가 수신되고 있습니다.",
            missing_msg="미수신된 종목 {missing}건 수신을 대기합니다.",
            first_msg="{name}종목에 대한 수신이 시작되었습니다.",
        ),
        _StageTracker(
            name="real_pm",
            channel="real",
            start_time=REAL_PM_START_TIME,
            end_time=END_TIME,
            start_msg="16:30분 : 실시간 체결조회를 시작합니다.",
            summary_after_sec=60,
            summary_msg="1분후 총 {recv}건의 실시간 체결조회가 되었습니다.",
            missing_msg="미수신된 종목이 {missing}건 있었습니다.",
        ),
        _StageTracker(
            name="exp_ah",
            channel="exp",
            start_time=EXP_AH_START_TIME,
            end_time=END_TIME,
            start_msg="16:00 : 시간외 단일가 거래시간으로 예상체결가 정보를 조회합니다.",
            summary_after_sec=600,
            summary_msg="10분단위 실시간 체결정보 수신 후 : {recv}종목에 대한 실시간 체결정보가 수신되었습니다.",
        ),
    ]

    stage_by_channel = {"exp": [], "real": []}
    for st in stages:
        stage_by_channel[st.channel].append(st)

    closed_logged = False
    resume_logged = False
    resume_done_logged = False

    save_lock = threading.Lock()

    def _save_logger() -> None:
        while not stop_counter.is_set() or not save_queue.empty():
            try:
                first = save_queue.get(timeout=0.5)
            except Exception:
                continue
            events = [first]
            window_start = time.time()
            while time.time() - window_start < 1.0:
                try:
                    events.append(save_queue.get_nowait())
                except Exception:
                    break
            ts = events[-1].get("ts", _fmt_ts())
            rate = sum(e.get("rate", 0.0) for e in events)
            if PRINT_OPTION == 1:
                for e in events:
                    sid = int(e.get("session_id", 0))
                    rows = int(e.get("rows", 0))
                    codes_in_flush = int(e.get("codes_in_flush", 0))
                    seen_codes = int(e.get("seen_codes", 0))
                    total_codes = int(e.get("total_codes", 0))
                    msg = (
                        f"[{e.get('ts', ts)}][{int(round(e.get('rate', 0.0))):03d}건/s] "
                        f"[save] ss={sid} rows={rows} codes={codes_in_flush}/{seen_codes}/{total_codes}"
                    )
                    with save_lock:
                        _log_status_stdout(msg)
            else:
                by_session: dict[int, dict[str, int]] = {}
                for e in events:
                    sid = int(e.get("session_id", 0))
                    if sid not in by_session:
                        by_session[sid] = {
                            "rows": 0,
                            "codes_in_flush": 0,
                            "seen_codes": 0,
                            "total_codes": 0,
                        }
                    by_session[sid]["rows"] += int(e.get("rows", 0))
                    by_session[sid]["codes_in_flush"] += int(e.get("codes_in_flush", 0))
                    by_session[sid]["seen_codes"] = max(
                        by_session[sid]["seen_codes"], int(e.get("seen_codes", 0))
                    )
                    by_session[sid]["total_codes"] = max(
                        by_session[sid]["total_codes"], int(e.get("total_codes", 0))
                    )
                parts = []
                for sid in sorted(by_session):
                    info = by_session[sid]
                    parts.append(
                        f"(ss{sid}, r_{info['rows']}, "
                        f"{info['codes_in_flush']}/{info['seen_codes']}/{info['total_codes']})"
                    )
                msg = f"[{ts}][{int(round(rate)):03d}건/s][SAVE]" + "".join(parts)
                with save_lock:
                    _log_status_stdout(msg)

    save_thread = threading.Thread(target=_save_logger, daemon=True)
    save_thread.start()

    while not _should_stop():
        now = _now_kst().time()
        if now >= MARKET_CLOSE_TIME and not closed_logged:
            msg = "정규장이 마감되어 장이 변경됩니다. (NXT거래는 지속되므로 wss수신은 계속 유지함.)"
            _log_status_stdout(msg)
            send_telegram(msg)
            closed_logged = True
        if now >= RESUME_WS_TIME and not resume_logged:
            msg = "16:00~18:00까지 시간외 단일가 거래데이터 수신을 위해 wss를 접속합니다."
            _log_status_stdout(msg)
            send_telegram(msg)
            resume_logged = True
        if now >= RESUME_WS_TIME and not resume_done_logged:
            msg = "예상체결가, 실시간체결가 wss 접속이 완료되었습니다."
            _log_status_stdout(msg)
            send_telegram(msg)
            session_paths = []
            for idx in range(1, len(sessions) + 1):
                session_path = DATA_DIR / _session_file_name(date_str, idx)
                save_path = str(session_path).replace("/home/ubuntu/Stoc_Kis", "...", 1)
                session_paths.append(save_path)
            _log_status_stdout("저장경로: " + ", ".join(session_paths))
            resume_done_logged = True
        for st in stages:
            if st.started_at is None and not st.skipped:
                if st.end_time is not None and now >= st.end_time:
                    st.skipped = True
                elif now >= st.start_time:
                    st.start(total_codes)
        while True:
            try:
                ch, code = event_queue.get_nowait()
            except Exception:
                break
            name = code_to_name.get(code, code)
            for st in stage_by_channel.get(ch, []):
                st.handle_code(code, name, total_codes)
        for st in stages:
            st.maybe_summary(total_codes)
        time.sleep(2)

    for p in procs:
        p.join(timeout=5)

    stop_counter.set()
    printer_thread.join(timeout=2)
    save_thread.join(timeout=2)

    msg_close = "18:01 : 시간외 단이락 거래가 종료되어 웹소켓 접속을 종료하고 파일 병합작업을 시작합니다."
    _log_status(msg_close)
    send_telegram(msg_close)
    final_path = _merge_session_files(date_str)
    if final_path.exists():
        short_final = str(final_path).replace("/home/ubuntu/Stoc_Kis", "...", 1)
        _log_status_stdout(f"최종 저장경로: {short_final}")
        _log_status_stdout("최종 저장을 완료했습니다.")
    else:
        _log_status_stdout("병합 실패로 최종 저장을 하지 못했습니다.")
    msg_done = "파일병합 저장이 완료되어 프로그램을 종료합니다."
    _log_status(msg_done)
    send_telegram(msg_done)
    end_ts = _fmt_ts()
    end_msg = f"[WSS][{start_ts}] ws_trading_data_collect.py 종료"
    print(end_msg, flush=True)
    send_telegram(end_msg)


if __name__ == "__main__":
    main()
