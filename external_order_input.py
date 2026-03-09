#!/usr/bin/env python3
"""외부주문 입력기 — config.json의 external_orders에 주문 추가.

사용법:
  python external_order_input.py

입력 방식:
  1) 건별 텍스트 입력 (탭/쉼표 구분)
  2) 엑셀에서 복사 후 붙여넣기 (여러 줄 한 번에)

컬럼 순서 (탭 또는 쉼표 구분):
  order  symbol  qty  amount  target_price  target  ord_type

  - order       : 1=매수, 2=매도  (필수)
  - symbol      : 종목명 또는 종목코드  (필수)
  - qty         : 수량 (0이면 amount로 자동 산출)
  - amount      : 금액 한도 (qty와 둘 중 하나)
  - target_price: 매수/매도 가격 (0 또는 -  = 시장가, 생략 시 0)
  - target      : 대상 계좌 (- 또는 생략 = all)
  - ord_type    : 05=장전시간외종가, 06=장후시간외종가, 07=시간외단일가 (생략=auto)

  ※ '-' 는 빈값(생략)으로 처리됩니다

예시:
  1	삼성전자	10
  1	삼성전자	0	10,000,000	-	a2
  2	005930	5	0	65000
  1	카카오	0	1,000,000	0	-	05
"""

import json
import sys
import os
import re

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")

VALID_ORD_TYPES = {
    "": "", "auto": "", "0": "",
    "05": "pre_market", "pre_market": "pre_market",
    "06": "post_market", "post_market": "post_market",
    "07": "overtime", "overtime": "overtime",
}
ORD_TYPE_LABEL = {
    "": "",
    "pre_market": "장전시간외종가(05)",
    "post_market": "장후시간외종가(06)",
    "overtime": "시간외단일가(07)",
}


def _is_blank(val: str) -> bool:
    """빈값 판정: '', '-', '--' 등."""
    return val in ("", "-", "--", ".")


def load_cfg() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_cfg(cfg: dict) -> None:
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def next_no(cfg: dict) -> int:
    """기존 external_orders에서 가장 큰 no + 1 반환."""
    orders = cfg.get("external_orders", [])
    if not orders:
        return 1
    max_no = max(int(o.get("no", 0)) for o in orders if isinstance(o, dict))
    return max_no + 1


def _parse_int(s: str) -> int | None:
    """쉼표/공백 제거 후 int 변환. 빈값이면 0."""
    if _is_blank(s):
        return 0
    try:
        return int(re.sub(r"[,\s]", "", s))
    except ValueError:
        return None


def parse_line(line: str) -> dict | None:
    """탭 또는 쉼표 구분 라인 → 주문 dict. 파싱 실패 시 None."""
    line = line.strip()
    if not line:
        return None

    # 탭 우선, 없으면 쉼표 구분
    if "\t" in line:
        parts = [p.strip() for p in line.split("\t")]
    else:
        parts = [p.strip() for p in line.split(",")]

    if len(parts) < 2:
        print(f"  [오류] 최소 2개 필드(order, symbol) 필요: {line}")
        return None

    # order (필수)
    try:
        order = int(parts[0])
    except ValueError:
        print(f"  [오류] order는 1(매수) 또는 2(매도): '{parts[0]}'")
        return None
    if order not in (1, 2):
        print(f"  [오류] order는 1(매수) 또는 2(매도): {order}")
        return None

    # symbol (필수) — 영문은 대문자로 변환
    symbol = parts[1].strip().upper()
    if not symbol or _is_blank(symbol):
        print(f"  [오류] symbol 비어있음")
        return None

    # qty (선택, 기본 0)
    qty = 0
    if len(parts) > 2:
        qty = _parse_int(parts[2])
        if qty is None:
            print(f"  [오류] qty 숫자 아님: '{parts[2]}'")
            return None

    # amount (선택, 기본 0)
    amount = 0
    if len(parts) > 3:
        amount = _parse_int(parts[3])
        if amount is None:
            print(f"  [오류] amount 숫자 아님: '{parts[3]}'")
            return None

    if qty <= 0 and amount <= 0:
        print(f"  [오류] qty 또는 amount 중 하나는 0보다 커야 함")
        return None

    # target_price (선택, 기본 0=시장가)
    target_price = 0
    if len(parts) > 4:
        target_price = _parse_int(parts[4])
        if target_price is None:
            print(f"  [오류] target_price 숫자 아님: '{parts[4]}'")
            return None

    # target (선택, 기본 all) + ord_type (선택, 기본 auto)
    # 나머지 필드에서 target과 ord_type을 자동 판별
    # - VALID_ORD_TYPES에 있으면 ord_type, 아니면 target으로 간주
    target = "all"
    ord_type = ""
    remaining = [p.strip() for p in parts[5:] if not _is_blank(p.strip())]
    for val in remaining:
        val_lower = val.lower()
        if val_lower in VALID_ORD_TYPES:
            ord_type = VALID_ORD_TYPES[val_lower]
        else:
            target = val

    return {
        "order": order,
        "symbol": symbol,
        "qty": qty,
        "amount": amount,
        "target_price": target_price,
        "ord_type": ord_type,
        "target": target,
    }


def format_order_display(o: dict, no: int) -> str:
    """주문 내용을 읽기 좋게 포맷."""
    action = "매수" if o["order"] == 1 else "매도"
    if o["qty"] > 0 and o["amount"] > 0:
        qty_str = f"수량={o['qty']:,}(한도 {o['amount']:,}원)"
    elif o["qty"] > 0:
        qty_str = f"수량={o['qty']:,}"
    else:
        qty_str = f"금액={o['amount']:,}원"
    tp = o["target_price"]
    price_str = "시장가" if tp == 0 else f"지정가={tp:,}"
    ot = o.get("ord_type", "")
    ot_label = ORD_TYPE_LABEL.get(ot, "")
    ot_disp = f"  [{ot_label}]" if ot_label else ""
    tgt = o.get("target", "all")
    tgt_disp = f"  계좌={tgt}" if tgt != "all" else "  계좌=전체"
    return f"  #{no:>3}  {action}  {o['symbol']:<12}  {qty_str}  {price_str}{ot_disp}{tgt_disp}"


def show_current_orders(cfg: dict) -> None:
    """현재 대기 중인 외부주문 표시."""
    orders = cfg.get("external_orders", [])
    if not orders:
        print("\n  현재 대기 중인 외부주문: 없음")
        return
    print(f"\n  현재 대기 중인 외부주문: {len(orders)}건")
    print("  " + "-" * 60)
    for o in orders:
        if not isinstance(o, dict):
            continue
        no = o.get("no", "?")
        action = "매수" if o.get("order") == 1 else "매도"
        result = o.get("result", "")
        result_disp = f"  [{result}]" if result else ""
        ot = o.get("ord_type", "")
        ot_label = ORD_TYPE_LABEL.get(ot, "")
        ot_disp = f"  [{ot_label}]" if ot_label else ""
        tgt = o.get("target", "all")
        tgt_disp = f"  계좌={tgt}" if tgt != "all" else ""
        print(f"  #{no}  {action}  {o.get('symbol', '?')}  "
              f"qty={o.get('qty', 0)}  amount={o.get('amount', 0):,}  "
              f"price={o.get('target_price', 0)}{ot_disp}{tgt_disp}{result_disp}")
    print("  " + "-" * 60)


def _cancel_order(no: int) -> None:
    """config.json에서 해당 no의 외부주문 삭제."""
    try:
        cfg = load_cfg()
        ext_orders = cfg.get("external_orders", [])
        if not ext_orders:
            print(f"  대기 중인 외부주문이 없습니다.")
            return
        found = [i for i, o in enumerate(ext_orders) if isinstance(o, dict) and o.get("no") == no]
        if not found:
            print(f"  #{no} 주문을 찾을 수 없습니다.")
            return
        removed = ext_orders.pop(found[0])
        sym = removed.get("symbol", "?")
        result = removed.get("result", "")
        if not ext_orders:
            cfg.pop("external_orders", None)
        save_cfg(cfg)
        print(f"  #{no} 삭제 완료: {sym} (result={result})")
        show_current_orders(cfg)
    except Exception as e:
        print(f"  [오류] 삭제 실패: {e}")


def main():
    print("=" * 64)
    print("  외부주문 입력기 — config.json → external_orders")
    print("=" * 64)
    print()
    print("  컬럼: order  symbol  qty  amount  target_price  target  ord_type")
    print("  구분: 탭 또는 쉼표  |  '-' = 빈값(생략)")
    print()
    print("  order       : 1=매수, 2=매도")
    print("  symbol      : 종목명 또는 종목코드")
    print("  qty         : 수량 (0이면 amount로 산출)")
    print("  amount      : 금액 한도 (qty와 둘 중 하나)")
    print("  target_price: 0 또는 - = 시장가 (생략 가능)")
    print("  target      : 대상 계좌 (- 또는 생략 = all)")
    print("  ord_type    : 05=장전시간외, 06=장후시간외, 07=시간외단일가")
    print("                (생략 = auto: 시장가/지정가 자동)")
    print()
    print("  빈 줄(Enter)로 확정 | 'q' 종료 | 'show' 대기 목록 | 'cancel #N' 삭제")
    print("-" * 64)

    # 현재 상태 표시
    try:
        cfg = load_cfg()
        show_current_orders(cfg)
    except Exception as e:
        print(f"  [경고] config.json 읽기 실패: {e}")

    while True:
        print()
        print("  주문 입력 (여러 줄 붙여넣기 가능, 빈 줄로 확정):")
        lines: list[str] = []
        while True:
            try:
                line = input("  > ")
            except (EOFError, KeyboardInterrupt):
                print("\n  종료.")
                return

            stripped = line.strip()
            if stripped.lower() == "q":
                print("  종료.")
                return
            if stripped.lower() == "show":
                try:
                    show_current_orders(load_cfg())
                except Exception as e:
                    print(f"  [오류] {e}")
                continue

            # cancel #N — 특정 no 삭제
            m = re.match(r"cancel\s+#?(\d+)", stripped, re.IGNORECASE)
            if m:
                _cancel_order(int(m.group(1)))
                continue

            if not stripped:
                break  # 빈 줄 → 확정
            lines.append(line)

        if not lines:
            continue

        # 파싱
        parsed: list[dict] = []
        for line in lines:
            o = parse_line(line)
            if o is not None:
                parsed.append(o)

        if not parsed:
            print("  유효한 주문이 없습니다.")
            continue

        # 확인
        try:
            cfg = load_cfg()
        except Exception as e:
            print(f"  [오류] config.json 읽기 실패: {e}")
            continue

        cur_no = next_no(cfg)
        print()
        print(f"  ── 입력 내용 확인 ({len(parsed)}건) ──")
        for i, o in enumerate(parsed):
            print(format_order_display(o, cur_no + i))
        print()

        try:
            confirm = input("  등록하시겠습니까? (y/n): ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n  종료.")
            return

        if confirm not in ("y", "yes", "ㅛ"):
            print("  취소됨.")
            continue

        # config.json에 추가
        try:
            cfg = load_cfg()  # 최신 상태 다시 읽기
            cur_no = next_no(cfg)
            ext_orders = cfg.get("external_orders", [])
            if not isinstance(ext_orders, list):
                ext_orders = []

            for i, o in enumerate(parsed):
                entry = {
                    "no": cur_no + i,
                    "order": o["order"],
                    "symbol": o["symbol"],
                    "qty": o["qty"],
                    "amount": o["amount"],
                    "target_price": o["target_price"],
                    "result": "",
                    "target_qty": 0,
                    "remain_qty": 0,
                }
                if o.get("ord_type"):
                    entry["ord_type"] = o["ord_type"]
                if o.get("target", "all") != "all":
                    entry["target"] = o["target"]
                ext_orders.append(entry)

            cfg["external_orders"] = ext_orders
            save_cfg(cfg)

            print(f"\n  {len(parsed)}건 등록 완료! (no #{cur_no}~#{cur_no + len(parsed) - 1})")
            show_current_orders(cfg)

        except Exception as e:
            print(f"  [오류] 저장 실패: {e}")


if __name__ == "__main__":
    main()
