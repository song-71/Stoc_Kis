"""
Strategy C — 볼린저 밴드 squeeze → expansion + 하단 반등 백테스트
Query_str_bb_expansion_simulation.py

매수 (AND):
  F1. 09:30 ≤ 시간 ≤ 14:30
  F2. bb_width 최근 100틱 최소값 < SQUEEZE (압축 이력)
  F3. 현재 bb_width > EXPAND (확장)
  F4. 최근 30틱 내 bidp1 < bb_lower 기록
  F5. 직전 틱 이탈 → 현재 복귀 (bidp1 ≥ bb_lower)
  F6. bidp1 > bidp1_prev (첫 양틱)
  F7. prdy_ctrt > 0 (상승 중)

매도 (OR):
  E1. bidp1 < bb_mid → 1차 절반 (여기선 단순화: 전량)
  E2. bidp1 < bb_lower → 전량
  E3. 손절 -3%
  E4. 트레일 -3% (+3% 상승 후 활성)
  E5. 14:55 마감 전량

BB: period=200 틱, K=2.0
bb_width 는 "정규화" — bb_width / stck_prpr × 100 (%) 사용 (절대값 문제 해결)
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path
from collections import deque

sys.path.append(str(Path(__file__).resolve().parents[1]))

import polars as pl

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"
WSS_DIR = DATA_DIR / "wss_data"

BB_PERIOD = 200
BB_K = 2.0
STOP_LOSS_PCT = 0.03
TRAIL_PCT = 0.03
TRAIL_ACTIVATE_PCT = 0.03

# 정규화 bb_width_pct = bb_width / price × 100 기준
BB_SQUEEZE_PCT = 2.0    # 2% (가격 대비)
BB_EXPAND_PCT = 3.5     # 3.5%
CROSS_LOOKBACK = 30
MIN_CTRT = 0.0


def simulate_day(date_str: str, verbose: bool = True) -> dict:
    path = WSS_DIR / f"{date_str}_wss_data.parquet"
    if not path.exists():
        return {"date": date_str, "trades": 0, "wins": 0, "total_pnl_pct": 0.0}

    df = pl.read_parquet(path, columns=[
        "mksc_shrn_iscd", "stck_cntg_hour", "stck_prpr",
        "prdy_ctrt", "bidp1", "acml_vol",
    ])
    df = df.with_columns([
        pl.col("stck_cntg_hour").cast(pl.Utf8).str.zfill(6),
        pl.col("stck_prpr").cast(pl.Float64, strict=False),
        pl.col("prdy_ctrt").cast(pl.Float64, strict=False),
        pl.col("bidp1").cast(pl.Float64, strict=False),
        pl.col("acml_vol").cast(pl.Float64, strict=False),
        pl.col("mksc_shrn_iscd").cast(pl.Utf8).str.zfill(6),
    ]).sort(["mksc_shrn_iscd", "stck_cntg_hour"]).filter(
        (pl.col("stck_cntg_hour") >= "093000")
        & (pl.col("stck_cntg_hour") <= "145500")
        & (pl.col("stck_prpr") > 0)
    )

    # BB (200틱 rolling) — 종목별
    df = df.with_columns([
        pl.col("stck_prpr").rolling_mean(BB_PERIOD, min_periods=BB_PERIOD)
        .over("mksc_shrn_iscd").alias("bb_mid"),
        pl.col("stck_prpr").rolling_std(BB_PERIOD, min_periods=BB_PERIOD)
        .over("mksc_shrn_iscd").alias("bb_std"),
    ])
    df = df.with_columns([
        (pl.col("bb_mid") + BB_K * pl.col("bb_std")).alias("bb_upper"),
        (pl.col("bb_mid") - BB_K * pl.col("bb_std")).alias("bb_lower"),
        (2 * BB_K * pl.col("bb_std")).alias("bb_width"),
        (2 * BB_K * pl.col("bb_std") / pl.col("stck_prpr") * 100).alias("bb_width_pct"),
    ])

    # 종목별 매매 시뮬 (Python 루프)
    trades = []
    by_code = df.partition_by("mksc_shrn_iscd", as_dict=True, maintain_order=True)
    for code_key, g in by_code.items():
        code = code_key[0] if isinstance(code_key, tuple) else code_key
        if g.height < BB_PERIOD + 50:
            continue
        # deque 기반 상태 (BB 사용 가능한 시점부터)
        width_history = deque(maxlen=100)
        lower_cross_history = deque(maxlen=CROSS_LOOKBACK)
        pos = None
        bidp1_prev = 0.0

        for r in g.iter_rows(named=True):
            bb_mid = r["bb_mid"]
            bb_lower = r["bb_lower"]
            bb_width_pct = r["bb_width_pct"]
            price = r["stck_prpr"]
            bidp = r["bidp1"] or price
            hhmmss = r["stck_cntg_hour"]

            if bb_mid is None or bb_lower is None or bb_width_pct is None:
                # BB 아직 형성 전 — 상태만 갱신
                bidp1_prev = bidp
                continue

            prev_cross = lower_cross_history[-1] if lower_cross_history else False
            is_cross_now = (bidp < bb_lower)
            had_cross_recent = any(lower_cross_history) if lower_cross_history else False
            min_width = min(width_history) if width_history else bb_width_pct

            # 포지션 없음 → 매수 판정
            if pos is None:
                if (
                    MIN_CTRT < (r["prdy_ctrt"] or 0)
                    and min_width < BB_SQUEEZE_PCT
                    and bb_width_pct > BB_EXPAND_PCT
                    and had_cross_recent
                    and prev_cross
                    and bidp >= bb_lower
                    and bidp > bidp1_prev
                ):
                    pos = {
                        "buy_price": price,
                        "buy_hhmmss": hhmmss,
                        "highest": bidp,
                        "buy_width_pct": bb_width_pct,
                    }
            else:
                # 매도 판정
                if bidp > pos["highest"]:
                    pos["highest"] = bidp
                exit_reason = None
                if bidp <= pos["buy_price"] * (1 - STOP_LOSS_PCT):
                    exit_reason = "손절-3%"
                elif bb_lower and bidp < bb_lower:
                    exit_reason = "bb_lower재이탈"
                elif (pos["highest"] >= pos["buy_price"] * (1 + TRAIL_ACTIVATE_PCT)
                      and bidp <= pos["highest"] * (1 - TRAIL_PCT)):
                    exit_reason = f"트레일-3%"
                elif bb_mid and bidp < bb_mid:
                    exit_reason = "bb_mid하회"
                elif hhmmss >= "145500":
                    exit_reason = "마감"

                if exit_reason:
                    pnl_pct = (bidp / pos["buy_price"] - 1) * 100
                    trades.append({
                        "code": code, "date": date_str,
                        "buy_price": pos["buy_price"], "sell_price": bidp,
                        "pnl_pct": pnl_pct, "exit_reason": exit_reason,
                    })
                    pos = None

            # 상태 갱신
            width_history.append(bb_width_pct)
            lower_cross_history.append(is_cross_now)
            bidp1_prev = bidp

        # 미청산 포지션 강제 마감
        if pos is not None and g.height > 0:
            last = g.row(-1, named=True)
            bidp = last["bidp1"] or last["stck_prpr"]
            pnl_pct = (bidp / pos["buy_price"] - 1) * 100
            trades.append({
                "code": code, "date": date_str,
                "buy_price": pos["buy_price"], "sell_price": bidp,
                "pnl_pct": pnl_pct, "exit_reason": "장마감자동청산",
            })

    wins = sum(1 for t in trades if t["pnl_pct"] > 0)
    total_pnl = sum(t["pnl_pct"] for t in trades)
    n_codes = df["mksc_shrn_iscd"].n_unique()

    if verbose:
        print(f"\n[{date_str}] 종목수={n_codes}, 거래={len(trades)}건, "
              f"승={wins}({wins/max(1,len(trades))*100:.1f}%), "
              f"총PnL={total_pnl:+.2f}%, 평균={total_pnl/max(1,len(trades)):+.3f}%")
        if trades:
            srt = sorted(trades, key=lambda x: x["pnl_pct"], reverse=True)
            print(f"  TOP3: ", end="")
            for t in srt[:3]:
                print(f"{t['code']}:{t['pnl_pct']:+.2f}%({t['exit_reason']})", end=" ")
            print()
            print(f"  BOT3: ", end="")
            for t in srt[-3:]:
                print(f"{t['code']}:{t['pnl_pct']:+.2f}%({t['exit_reason']})", end=" ")
            print()

    return {"date": date_str, "trades": len(trades), "wins": wins,
            "total_pnl_pct": total_pnl, "trade_list": trades}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default="260421,260422,260423")
    args = ap.parse_args()
    dates = [d.strip() for d in args.dates.split(",")]
    print(f"Strategy C — BB expansion 백테스트: {dates}")
    print(f"(정규화 bb_width_pct = bb_width/price*100, SQUEEZE<{BB_SQUEEZE_PCT}%, EXPAND>{BB_EXPAND_PCT}%)")
    print("=" * 70)

    all_r = []
    for d in dates:
        all_r.append(simulate_day(d, verbose=True))

    total_trades = sum(r["trades"] for r in all_r)
    total_wins = sum(r["wins"] for r in all_r)
    total_pnl = sum(r["total_pnl_pct"] for r in all_r)
    print("\n" + "=" * 70)
    print(f"[총합] 거래={total_trades}건, 승률={total_wins/max(1,total_trades)*100:.1f}%, "
          f"총PnL={total_pnl:+.2f}%, 거래당평균={total_pnl/max(1,total_trades):+.3f}%")
    print("=" * 70)

    all_trades = [t for r in all_r for t in r.get("trade_list", [])]
    if all_trades:
        from collections import Counter
        cnt = Counter(t["exit_reason"] for t in all_trades)
        print(f"\n[사유별 분포]")
        for reason, n in cnt.most_common():
            rt = [t for t in all_trades if t["exit_reason"] == reason]
            avg = sum(t["pnl_pct"] for t in rt) / max(1, n)
            print(f"  {reason:<20} {n:>4}건 평균 {avg:+.2f}%")


if __name__ == "__main__":
    main()
