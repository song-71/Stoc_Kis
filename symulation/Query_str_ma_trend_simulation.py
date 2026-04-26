"""
Strategy B — MA trend-following 백테스트 (실제 구현)
Query_str_ma_trend_simulation.py

매수: ma50 > ma500 골든크로스 순간 + 15~29% 구간 + 전일 <10% + 거래량 ≥ 100k
매도: ma10 < ma500 데드크로스 OR -3% 손절 OR 고점 대비 -3% 트레일 OR 14:55 마감

데이터: data/wss_data/YYMMDD_wss_data.parquet (WSS 틱)
스키마: mksc_shrn_iscd, stck_cntg_hour, stck_prpr, prdy_ctrt, stck_oprc, bidp1, acml_vol

사용법:
  python Query_str_ma_trend_simulation.py --dates 260421,260422,260423
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import polars as pl

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"
WSS_DIR = DATA_DIR / "wss_data"

# 전략 상수
BUY_CTRT_MIN = 15.0
BUY_CTRT_MAX = 29.0
MIN_VOLUME = 100_000
STOP_LOSS_PCT = 0.03
TRAIL_PCT = 0.03
TRAIL_ACTIVATE_PCT = 0.03

# MA periods
MA_PERIODS = [10, 50, 500]


def simulate_day(date_str: str, verbose: bool = True) -> dict:
    """하루치 Strategy B 시뮬레이션."""
    path = WSS_DIR / f"{date_str}_wss_data.parquet"
    if not path.exists():
        print(f"[{date_str}] 파일 없음: {path.name}")
        return {"date": date_str, "trades": 0, "wins": 0, "total_pnl_pct": 0.0, "codes": 0}

    # 로드 + 필요 컬럼만
    df = pl.read_parquet(path, columns=[
        "mksc_shrn_iscd", "stck_cntg_hour", "stck_prpr",
        "prdy_ctrt", "stck_oprc", "bidp1", "acml_vol",
    ])
    # 타입 변환 + 정렬
    df = df.with_columns([
        pl.col("stck_cntg_hour").cast(pl.Utf8).str.zfill(6),
        pl.col("stck_prpr").cast(pl.Float64, strict=False),
        pl.col("prdy_ctrt").cast(pl.Float64, strict=False),
        pl.col("stck_oprc").cast(pl.Float64, strict=False),
        pl.col("bidp1").cast(pl.Float64, strict=False),
        pl.col("acml_vol").cast(pl.Float64, strict=False),
        pl.col("mksc_shrn_iscd").cast(pl.Utf8).str.zfill(6),
    ]).sort(["mksc_shrn_iscd", "stck_cntg_hour"])

    # 시간 필터: 09:30 ~ 14:55 (HHMMSS 문자열)
    df = df.filter(
        (pl.col("stck_cntg_hour") >= "093000")
        & (pl.col("stck_cntg_hour") <= "145500")
    )

    # 종목별 EMA 계산 (ewm_mean over code)
    for n in MA_PERIODS:
        alpha = 2.0 / (n + 1)
        df = df.with_columns(
            pl.col("stck_prpr").ewm_mean(alpha=alpha, adjust=False)
            .over("mksc_shrn_iscd")
            .alias(f"ema{n}")
        )

    # 직전 틱 ma50, ma500 (shift)
    df = df.with_columns([
        pl.col("ema50").shift(1).over("mksc_shrn_iscd").alias("ema50_prev"),
        pl.col("ema500").shift(1).over("mksc_shrn_iscd").alias("ema500_prev"),
    ])

    # 매수 신호: 골든크로스 edge + 구간 + 거래량
    df = df.with_columns(
        (
            (pl.col("ema50_prev") <= pl.col("ema500_prev"))
            & (pl.col("ema50") > pl.col("ema500"))
            & (pl.col("prdy_ctrt") >= BUY_CTRT_MIN)
            & (pl.col("prdy_ctrt") < BUY_CTRT_MAX)
            & (pl.col("acml_vol") >= MIN_VOLUME)
            & (pl.col("ema50") > 0) & (pl.col("ema500") > 0)
        ).alias("buy_signal")
    )

    # 매도 신호: 데드크로스
    df = df.with_columns(
        ((pl.col("ema10") < pl.col("ema500")) & (pl.col("ema500") > 0)).alias("sell_signal")
    )

    # 종목별로 매매 시뮬 (순차 루프 — 포지션 상태 추적)
    trades = []
    by_code = df.partition_by("mksc_shrn_iscd", as_dict=True, maintain_order=True)
    for code_key, g in by_code.items():
        code = code_key[0] if isinstance(code_key, tuple) else code_key
        pos = None  # dict: buy_price, buy_hhmmss, highest
        for r in g.iter_rows(named=True):
            price = r["stck_prpr"]
            bidp = r["bidp1"] or price
            hhmmss = r["stck_cntg_hour"]
            if price <= 0:
                continue

            if pos is None:
                if r["buy_signal"]:
                    pos = {
                        "buy_price": price,
                        "buy_hhmmss": hhmmss,
                        "highest": bidp,
                    }
            else:
                # 고점 갱신
                if bidp > pos["highest"]:
                    pos["highest"] = bidp
                exit_reason = None
                # E2 손절
                if bidp <= pos["buy_price"] * (1 - STOP_LOSS_PCT):
                    exit_reason = "손절-3%"
                # E3 트레일
                elif (pos["highest"] >= pos["buy_price"] * (1 + TRAIL_ACTIVATE_PCT)
                      and bidp <= pos["highest"] * (1 - TRAIL_PCT)):
                    exit_reason = f"트레일-3%(고점{int(pos['highest'])})"
                # E1 데드크로스
                elif r["sell_signal"]:
                    exit_reason = "데드크로스"
                # E4 마감 (14:55 이후 진입한 rows 없음 — 필터됨)
                elif hhmmss >= "145500":
                    exit_reason = "마감"

                if exit_reason:
                    pnl_pct = (bidp / pos["buy_price"] - 1) * 100
                    trades.append({
                        "code": code, "date": date_str,
                        "buy_hhmmss": pos["buy_hhmmss"], "buy_price": pos["buy_price"],
                        "sell_hhmmss": hhmmss, "sell_price": bidp,
                        "highest": pos["highest"], "pnl_pct": pnl_pct,
                        "exit_reason": exit_reason,
                    })
                    pos = None
        # 종목 끝까지 도달해도 포지션 남아있으면 마지막 가격으로 청산
        if pos is not None and len(g) > 0:
            last = g.row(-1, named=True)
            bidp = last["bidp1"] or last["stck_prpr"]
            pnl_pct = (bidp / pos["buy_price"] - 1) * 100
            trades.append({
                "code": code, "date": date_str,
                "buy_hhmmss": pos["buy_hhmmss"], "buy_price": pos["buy_price"],
                "sell_hhmmss": last["stck_cntg_hour"], "sell_price": bidp,
                "highest": pos["highest"], "pnl_pct": pnl_pct,
                "exit_reason": "장마감자동청산",
            })

    wins = sum(1 for t in trades if t["pnl_pct"] > 0)
    total_pnl = sum(t["pnl_pct"] for t in trades)
    n_codes = df["mksc_shrn_iscd"].n_unique()

    if verbose:
        print(f"\n[{date_str}] 종목수={n_codes}, 거래={len(trades)}건, "
              f"승={wins}({wins/max(1,len(trades))*100:.1f}%), "
              f"총PnL={total_pnl:+.2f}%, 평균={total_pnl/max(1,len(trades)):+.2f}%")
        # 상세 상위 5건 + 하위 5건
        if trades:
            srt = sorted(trades, key=lambda x: x["pnl_pct"], reverse=True)
            print(f"  TOP5: ", end="")
            for t in srt[:5]:
                print(f"{t['code']}:{t['pnl_pct']:+.2f}%({t['exit_reason']})", end=" ")
            print()
            print(f"  BOT5: ", end="")
            for t in srt[-5:]:
                print(f"{t['code']}:{t['pnl_pct']:+.2f}%({t['exit_reason']})", end=" ")
            print()

    return {
        "date": date_str, "codes": n_codes,
        "trades": len(trades), "wins": wins,
        "total_pnl_pct": total_pnl,
        "avg_pnl_pct": total_pnl / max(1, len(trades)),
        "trade_list": trades,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default="260421,260422,260423",
                    help="쉼표 구분 YYMMDD 리스트")
    args = ap.parse_args()

    dates = [d.strip() for d in args.dates.split(",")]
    print(f"Strategy B — MA trend 백테스트: {dates}")
    print("=" * 70)

    all_results = []
    for d in dates:
        r = simulate_day(d, verbose=True)
        all_results.append(r)

    # 총합
    total_trades = sum(r["trades"] for r in all_results)
    total_wins = sum(r["wins"] for r in all_results)
    total_pnl = sum(r["total_pnl_pct"] for r in all_results)

    print()
    print("=" * 70)
    print(f"[총합] 일수={len(dates)}, 거래={total_trades}건, "
          f"승률={total_wins/max(1,total_trades)*100:.1f}%, "
          f"총PnL={total_pnl:+.2f}%, "
          f"일평균={total_pnl/len(dates):+.2f}%, "
          f"거래당평균={total_pnl/max(1,total_trades):+.2f}%")
    print("=" * 70)

    # 모든 거래 집계
    all_trades = [t for r in all_results for t in r.get("trade_list", [])]
    if all_trades:
        print(f"\n[사유별 분포]")
        from collections import Counter
        cnt = Counter(t["exit_reason"] for t in all_trades)
        for reason, n in cnt.most_common():
            reason_trades = [t for t in all_trades if t["exit_reason"] == reason]
            avg = sum(t["pnl_pct"] for t in reason_trades) / max(1, n)
            print(f"  {reason:<20} {n:>4}건 평균 {avg:+.2f}%")


if __name__ == "__main__":
    main()
