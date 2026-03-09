"""변동성완화장치(VI) 현황 조회 — 간편 버전 [국내주식-055]"""
import json, os, sys, time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kis_API_ohlcv_download_Utils import DEFAULT_BASE_URL, KisClient, KisConfig

# ══════════════════════════════════════════════════════════════════
#  [조회 옵션] — 여기서 직접 수정
# ══════════════════════════════════════════════════════════════════
OPT_CODE      = ""          # 종목코드 (빈값=전체, 예: "005930")
OPT_MARKET    = "0"         # 시장구분  0:전체 / K:코스피 / Q:코스닥
OPT_DIRECTION = "1"         # 방향구분  0:전체 / 1:상승 / 2:하락
OPT_VI_TYPE   = "0"         # VI종류   0:전체 / 1:정적 / 2:동적 / 3:정적&동적
OPT_DATE      = "20260303"  # 영업일자  YYYYMMDD (빈값=당일)
# ══════════════════════════════════════════════════════════════════

_KST = timezone(timedelta(hours=9))
_BASE = os.path.dirname(os.path.abspath(__file__))
_VI_STATUS = {"N": "해제", "Y": "발동중", "0": "해제", "1": "정적VI", "2": "동적VI", "3": "정적&동적"}
_VI_KIND   = {"1": "정적VI", "2": "동적VI", "3": "정적&동적"}


def _client():
    with open(os.path.join(_BASE, "config.json"), encoding="utf-8") as f:
        cfg = json.load(f)
    acct = cfg["users"][cfg["default_user"]]["accounts"]["main"]
    return KisClient(KisConfig(
        appkey=acct["appkey"], appsecret=acct["appsecret"],
        base_url=cfg.get("base_url", DEFAULT_BASE_URL),
        token_cache_path=os.path.join(_BASE, "kis_token_main.json"),
    ))


def _fmt_time(t):
    t = str(t).strip()
    return f"{t[:2]}:{t[2:4]}:{t[4:6]}" if len(t) == 6 else t


def _fmt_num(v):
    try:
        n = float(v)
        return f"{int(n):,}" if n == int(n) else f"{n:,.2f}"
    except (ValueError, TypeError):
        return str(v)


def query_vi(client):
    date = OPT_DATE or datetime.now(_KST).strftime("%Y%m%d")
    url = f"{client.cfg.base_url}/uapi/domestic-stock/v1/quotations/inquire-vi-status"
    params = {
        "FID_COND_SCR_DIV_CODE": "20139",
        "FID_INPUT_ISCD": OPT_CODE,
        "FID_MRKT_CLS_CODE": OPT_MARKET,
        "FID_DIV_CLS_CODE": OPT_DIRECTION,
        "FID_RANK_SORT_CLS_CODE": OPT_VI_TYPE,
        "FID_INPUT_DATE_1": date,
        "FID_TRGT_CLS_CODE": "",
        "FID_TRGT_EXLS_CLS_CODE": "",
    }

    rows = []
    tr_cont = ""
    for _ in range(10):
        headers = client._headers(tr_id="FHPST01390000")
        headers["tr_cont"] = "N" if tr_cont else ""
        r = client.session.get(url, headers=headers, params=params, timeout=10)
        j = r.json()
        if str(j.get("rt_cd")) != "0":
            print(f"[오류] {j.get('msg1', '')}")
            break
        output = j.get("output") or []
        if not output:
            break
        rows.extend(output)
        if r.headers.get("tr_cont") == "M":
            tr_cont = "N"
            time.sleep(0.5)
        else:
            break
    return rows, date


def display(rows, date):
    mkt = {"0": "전체", "K": "코스피", "Q": "코스닥"}.get(OPT_MARKET, OPT_MARKET)
    dirn = {"0": "전체", "1": "상승", "2": "하락"}.get(OPT_DIRECTION, OPT_DIRECTION)
    print(f"\n{'='*90}")
    print(f"  VI 현황  |  날짜: {date}  |  시장: {mkt}  |  방향: {dirn}  |  {len(rows)}건")
    print(f"{'='*90}")
    print(f"{'No':>4}  {'종목명':<14} {'코드':^8} {'상태':<8} {'종류':<8} "
          f"{'발동시간':^10} {'해제시간':^10} {'발동가':>10} {'기준가':>10} {'괴리율':>8} {'횟수':>4}")
    print(f"{'-'*90}")

    for i, r in enumerate(rows, 1):
        st = _VI_STATUS.get(r.get("vi_cls_code", ""), r.get("vi_cls_code", ""))
        kd = _VI_KIND.get(r.get("vi_kind_code", ""), r.get("vi_kind_code", ""))
        stnd = r.get("vi_stnd_prc", "")
        dprt = r.get("vi_dprt", "")
        if not str(stnd).strip() or str(stnd).strip() == "0":
            stnd = r.get("vi_dmc_stnd_prc", "")
            dprt = r.get("vi_dmc_dprt", "")
        dprt_s = f"{float(dprt):.2f}%" if dprt and str(dprt).strip() not in ("", "0") else ""
        mk = " *" if r.get("vi_cls_code", "") in ("Y", "1", "2", "3") else "  "

        print(f"{i:>4}{mk}{str(r.get('hts_kor_isnm','')).strip()[:12]:<14} "
              f"{str(r.get('mksc_shrn_iscd','')).strip():^8} {st:<8} {kd:<8} "
              f"{_fmt_time(r.get('cntg_vi_hour','')):^10} {_fmt_time(r.get('vi_cncl_hour','')):^10} "
              f"{_fmt_num(r.get('vi_prc','')):>10} {_fmt_num(stnd):>10} {dprt_s:>8} "
              f"{str(r.get('vi_count','')).strip():>4}")

    print(f"{'='*90}")


if __name__ == "__main__":
    c = _client()
    rows, date = query_vi(c)
    if rows:
        display(rows, date)
    else:
        print(f"조회된 VI 데이터 없음 (날짜: {OPT_DATE or '당일'})")
