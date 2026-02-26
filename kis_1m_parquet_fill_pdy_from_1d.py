"""
1m parquet 파일에 pdy_close/tdy_ret/pdy_ret을 1d DB 기준으로 보정합니다.
 - 대상: /home/ubuntu/Stoc_Kis/data/1m_data/*_1m_chart_DB_parquet.parquet
 - 기준: 1d DB에서 해당 일자 이전(date < target_date)의 마지막 데이터
 - 출력: 원본 파일을 직접 교정(덮어쓰기)
"""

import argparse
from pathlib import Path

import polars as pl


def _load_1d_df(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"1d parquet not found: {path}")
    df = pl.read_parquet(path)
    code_col = "code" if "code" in df.columns else "symbol"
    df = df.with_columns(
        [
            pl.col(code_col).cast(pl.Utf8).str.strip_chars().str.zfill(6).alias("code"),
            pl.col("date").cast(pl.Utf8).str.strip_chars().alias("date"),
        ]
    ).with_columns(
        [
            pl.col("date").str.replace_all("-", "").cast(pl.Int64).alias("date_int"),
            pl.col("close").cast(pl.Float64),
            pl.col("pdy_close").cast(pl.Float64),
        ]
    )
    df = df.with_columns(
        [
            pl.when(pl.col("pdy_close") > 0)
            .then((pl.col("close") - pl.col("pdy_close")) / pl.col("pdy_close"))
            .otherwise(None)
            .alias("tdy_ret")
        ]
    )
    df = df.with_columns(
        [
            pl.col("pdy_close").shift(1).over("code").alias("p2dy_close")
        ]
    ).with_columns(
        [
            pl.when(pl.col("p2dy_close") > 0)
            .then((pl.col("pdy_close") / pl.col("p2dy_close")) - 1.0)
            .otherwise(None)
            .alias("pdy_ret")
        ]
    )
    return df.select(["code", "date_int", "pdy_close", "tdy_ret", "pdy_ret"])


def _build_pdy_map(df_1d: pl.DataFrame, target_date: str) -> pl.DataFrame:
    target_int = int(target_date)
    df = df_1d.filter(pl.col("date_int") < target_int)
    if df.is_empty():
        return pl.DataFrame({"code": [], "pdy_close": [], "tdy_ret": [], "pdy_ret": []})
    df = df.sort(["code", "date_int"]).group_by("code").agg(
        [
            pl.col("pdy_close").last().alias("pdy_close"),
            pl.col("tdy_ret").last().alias("tdy_ret"),
            pl.col("pdy_ret").last().alias("pdy_ret"),
        ]
    )
    return df


def _parse_date_from_name(name: str) -> str | None:
    stem = Path(name).stem
    if len(stem) >= 8 and stem[:8].isdigit():
        return stem[:8]
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="/home/ubuntu/Stoc_Kis/data/1m_data")
    parser.add_argument(
        "--one-day-db",
        default="/home/ubuntu/Stoc_Kis/data/1d_data/kis_1d_unified_parquet_DB.parquet",
    )
    parser.add_argument("--output-dir", default="")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    if not out_dir.exists():
        raise FileNotFoundError(f"out dir not found: {out_dir}")
    output_dir = out_dir

    files = sorted(out_dir.glob("*_1m_chart_DB_parquet.parquet"))
    if not files:
        print(f"[skip] no parquet files in {out_dir}")
        return

    df_1d = _load_1d_df(Path(args.one_day_db))
    cache: dict[str, pl.DataFrame] = {}

    for path in files:
        target_date = _parse_date_from_name(path.name)
        if not target_date:
            print(f"[skip] date parse failed: {path.name}")
            continue

        if target_date not in cache:
            cache[target_date] = _build_pdy_map(df_1d, target_date)
        pdy_map = cache[target_date]

        try:
            df_1m = pl.read_parquet(path)
        except Exception as e:
            print(f"[skip] parquet read failed: {path} error={e}")
            continue
        if "code" not in df_1m.columns:
            print(f"[skip] code column missing: {path.name}")
            continue
        df_1m = df_1m.with_columns(
            pl.col("code").cast(pl.Utf8).str.strip_chars().str.zfill(6).alias("code")
        )

        joined = df_1m.join(pdy_map, on="code", how="left", suffix="_pdy")
        if "pdy_close_pdy" in joined.columns:
            if "pdy_close" in df_1m.columns:
                joined = joined.with_columns(
                    pl.coalesce([pl.col("pdy_close_pdy"), pl.col("pdy_close")]).alias("pdy_close")
                )
            else:
                joined = joined.with_columns(pl.col("pdy_close_pdy").alias("pdy_close"))
        if "tdy_ret_pdy" in joined.columns:
            if "tdy_ret" in df_1m.columns:
                joined = joined.with_columns(
                    pl.coalesce([pl.col("tdy_ret_pdy"), pl.col("tdy_ret")]).alias("tdy_ret")
                )
            else:
                joined = joined.with_columns(pl.col("tdy_ret_pdy").alias("tdy_ret"))
        if "pdy_ret_pdy" in joined.columns:
            if "pdy_ret" in df_1m.columns:
                joined = joined.with_columns(
                    pl.coalesce([pl.col("pdy_ret_pdy"), pl.col("pdy_ret")]).alias("pdy_ret")
                )
            else:
                joined = joined.with_columns(pl.col("pdy_ret_pdy").alias("pdy_ret"))

        if "pdy_close" in joined.columns and "close" in joined.columns:
            joined = joined.with_columns(
                pl.when(pl.col("pdy_close") > 0)
                .then((pl.col("close").cast(pl.Float64) - pl.col("pdy_close")) / pl.col("pdy_close"))
                .otherwise(None)
                .alias("tdy_ret")
            )
        drop_legacy = [c for c in ("prev_ret", "prev_close") if c in joined.columns]
        if drop_legacy:
            joined = joined.drop(drop_legacy)

        joined = joined.drop([c for c in joined.columns if c.endswith("_pdy")])
        out_path = output_dir / path.name
        joined.write_parquet(out_path)
        print(f"[ok] {path.name} -> {out_path} (in-place)")


if __name__ == "__main__":
    main()
