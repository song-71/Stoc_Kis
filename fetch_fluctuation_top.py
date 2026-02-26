import time
from datetime import datetime, timedelta, time as dt_time
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from kis_API_ohlcv_download_Utils import DEFAULT_BASE_URL, KisClient, KisConfig
from kis_utils import load_config

KST = ZoneInfo("Asia/Seoul")
OUT_DIR = "/home/ubuntu/Stoc_Kis/data/fetch_top_list"
TOP_N = 50


def _fetch_fluctuation_top(client: KisClient, top_n: int = 50, market_div: str = "J") -> list[dict]:
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/ranking/fluctuation"
    headers = client._headers(tr_id="FHPST01700000")
    params = {
        "fid_cond_mrkt_div_code": market_div,
        "fid_cond_scr_div_code": "20170",
        "fid_input_iscd": "0000",
        "fid_rank_sort_cls_code": "0",
        "fid_input_cnt_1": "0",
        "fid_prc_cls_code": "0",
        "fid_input_price_1": "",
        "fid_input_price_2": "",
        "fid_vol_cnt": "",
        "fid_trgt_cls_code": "0",
        "fid_trgt_exls_cls_code": "0",
        "fid_div_cls_code": "0",
        "fid_rsfl_rate1": "",
        "fid_rsfl_rate2": "",
    }
    r = requests.get(url, headers=headers, params=params, timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("rt_cd")) != "0":
        raise RuntimeError(f"[등락률순위] 실패: {j.get('msg1')} raw={j}")
    output = j.get("output") or []
    if isinstance(output, dict):
        output = [output]
    if isinstance(output, list) and top_n > 0:
        return output[:top_n]
    return output


def _build_schedule(today: datetime) -> list[datetime]:
    base = today.date()
    fixed = [
        dt_time(8, 50),
        dt_time(8, 59),
        dt_time(9, 0),
        dt_time(9, 5),
        dt_time(9, 10),
        dt_time(9, 15),
    ]
    times = [datetime.combine(base, t, tzinfo=KST) for t in fixed]

    start = datetime.combine(base, dt_time(9, 30), tzinfo=KST)
    end = datetime.combine(base, dt_time(15, 0), tzinfo=KST)
    cur = start
    while cur <= end:
        times.append(cur)
        cur += timedelta(minutes=30)

    return sorted(set(times))


def _save_csv(rows: list[dict], ts: datetime) -> str:
    df = pd.DataFrame(rows)
    fname = ts.strftime("Fetch_fluctuation_top_%y%m%d_%H%M.csv")
    out_path = f"{OUT_DIR}/{fname}"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path


def main() -> None:
    cfg = load_config()
    appkey = cfg.get("appkey")
    appsecret = cfg.get("appsecret")
    if not appkey or not appsecret:
        raise ValueError("appkey/appsecret이 필요합니다. config.json에 설정하세요.")
    base_url = cfg.get("base_url") or DEFAULT_BASE_URL
    custtype = cfg.get("custtype") or "P"
    market_div = cfg.get("market_div") or "J"

    kis_cfg = KisConfig(
        appkey=appkey,
        appsecret=appsecret,
        base_url=base_url,
        custtype=custtype,
        market_div=market_div,
    )
    client = KisClient(kis_cfg)

    now = datetime.now(KST)
    schedule = _build_schedule(now)

    remaining = [t for t in schedule if t > now]
    if not remaining:
        print("No remaining schedule times for today.")
        return

    for target in remaining:
        while True:
            now = datetime.now(KST)
            if now >= target:
                break
            time.sleep(min(1.0, (target - now).total_seconds()))

        rows = _fetch_fluctuation_top(client, top_n=TOP_N, market_div=market_div)
        out_path = _save_csv(rows, target)
        print(f"[{target.strftime('%y%m%d_%H%M')}] saved {len(rows)} rows -> {out_path}")


if __name__ == "__main__":
    main()
