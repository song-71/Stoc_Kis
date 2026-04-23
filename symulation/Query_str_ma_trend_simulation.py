"""
Strategy B — MA trend-following 백테스트
Query_str_ma_trend_simulation.py

[전략]
매수: ma50 > ma500 골든크로스 순간 (edge-trigger) + 15~29% 구간 + 전일 <10% + 거래량 ≥ 100k
매도: ma10 < ma500 데드크로스 OR -3% 손절 OR 고점 대비 -3% 트레일 OR 14:55 마감

[데이터 소스]
- data/wss_data/YYMMDD_wss_data.parquet (틱 데이터) 또는
- data/kis_1m_parquet/ (1분봉, 보조)

[사용법]
  python Query_str_ma_trend_simulation.py --start 20260320 --end 20260423

[TODO — 실제 구현 필요]
1. WSS 틱 parquet 로드 후 종목별 EMA(10/50/500) 계산
2. 종목별 일자별 매매 시뮬레이션:
   - 매 틱마다 buy_signal 체크
   - 포지션 보유 중이면 매 틱 exit_signal 체크
   - 슬리피지 가정 (매수: ask, 매도: bid)
3. 결과 집계:
   - 일자별 거래 건수, 승률, 평균 수익률, MDD
   - 장 시장국면 분류 (KOSPI 일등락률 기준):
     * 상승장 (> +0.5%)
     * 횡보 (-0.5% ~ +0.5%)
     * 하락장 (< -0.5%)
   - 국면별 성과 분리 리포트
4. 임계값 그리드 서치 (선택):
   - MA_TREND_BUY_CTRT_MIN: 10/12/15/18
   - 손절: -2/-3/-5%
   - 트레일 ACTIVATE: +2/+3/+5%
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).resolve().parents[1]))

DEFAULT_START = "20260320"
DEFAULT_END = "20260423"

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"
WSS_DIR = DATA_DIR / "wss_data"
PARQUET_1M = DATA_DIR / "kis_1m_parquet"

# ── 전략 상수 (ws_realtime_tr_str1.py 와 일치) ──
BUY_CTRT_MIN = 15.0
BUY_CTRT_MAX = 29.0
PREV_CTRT_MAX = 10.0
MIN_VOLUME = 100_000
STOP_LOSS_PCT = 0.03
TRAIL_PCT = 0.03
TRAIL_ACTIVATE_PCT = 0.03


def load_daily_ticks(date_str: str):
    """해당 일자 WSS 틱 데이터 로드."""
    # TODO: parquet 읽기 + 종목별 필터
    path = WSS_DIR / f"{date_str[2:]}_wss_data.parquet"
    if not path.exists():
        return None
    # import polars as pl
    # return pl.read_parquet(path)
    raise NotImplementedError("[TODO] WSS 틱 데이터 로드 구현 필요")


def simulate_day(date_str: str) -> dict:
    """하루치 매매 시뮬레이션."""
    # TODO:
    # 1. 일자 틱 로드
    # 2. 종목별 EMA(10/50/500) 계산
    # 3. 매 틱 매수 신호 검사 → 포지션 생성
    # 4. 포지션 보유 중이면 매 틱 exit 검사 → 청산
    # 5. 일자 결과 반환 (trades, wins, total_return)
    raise NotImplementedError("[TODO] 일별 시뮬레이션 구현 필요")


def aggregate_results(per_day_results: list[dict]) -> dict:
    """전체 결과 집계."""
    # TODO: 승률, 평균 수익률, MDD, 국면별 분리
    return {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--end", default=DEFAULT_END)
    args = ap.parse_args()

    print(f"[Strategy B] MA trend backtest: {args.start} ~ {args.end}")
    print("[TODO] 구현 필요 — load_daily_ticks, simulate_day, aggregate_results")
    print("현재는 skeleton 상태. 실제 구현 후 결과 출력.")


if __name__ == "__main__":
    main()
