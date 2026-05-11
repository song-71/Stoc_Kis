"""Web Console — Todo + External Order (Flask + SQLite)

실행:
  nohup /home/ubuntu/Stoc_Kis/venv/bin/python3 /home/ubuntu/Stoc_Kis/web-console/app.py > /tmp/web-console.log 2>&1 &
프로세스 확인
  pgrep -af app.py  

일괄 종료:현재 떠있는 여러 프로세스를 동시에 종료하는 명령어
  pkill -f app.py  

브라우저 접속:
  http://<EC2_PUBLIC_IP>:5000             # 외부 (보안그룹 5000 포트 오픈 필요)
  http://localhost:5000                   # 서버 내부

종료:
  Ctrl+C (포그라운드) / pkill -f "web-console/app.py" (백그라운드)
"""
import sqlite3
import json
import re
from pathlib import Path
from flask import Flask, render_template, request, jsonify

_BASE_DIR = Path(__file__).resolve().parent
app = Flask(__name__, template_folder=str(_BASE_DIR / "templates"))
DB_PATH = "todo.db"
CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.json"
ORDER_PASSWORD = "dmswjd@21"  # 주문 비밀번호

# ── SQLite (Todo) ──────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS todos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                category TEXT DEFAULT '',
                priority INTEGER DEFAULT 0,
                done INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # 기존 테이블에 category/priority 없으면 추가
        cols = [r[1] for r in conn.execute("PRAGMA table_info(todos)").fetchall()]
        if "category" not in cols:
            conn.execute("ALTER TABLE todos ADD COLUMN category TEXT DEFAULT ''")
        if "priority" not in cols:
            conn.execute("ALTER TABLE todos ADD COLUMN priority INTEGER DEFAULT 0")


init_db()


# ── Todo API ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/todos", methods=["GET"])
def get_todos():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM todos ORDER BY done ASC, priority DESC, id DESC"
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/todos", methods=["POST"])
def add_todo():
    data = request.json or {}
    title = data.get("title", "").strip()
    if not title:
        return jsonify({"error": "title required"}), 400
    category = data.get("category", "").strip()
    priority = data.get("priority", "보통")
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO todos (title, category, priority) VALUES (?, ?, ?)",
            (title, category, priority),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM todos WHERE id=?", (cur.lastrowid,)).fetchone()
    return jsonify(dict(row)), 201


@app.route("/api/todos/<int:todo_id>", methods=["PUT"])
def update_todo(todo_id):
    data = request.json or {}
    with get_db() as conn:
        if "done" in data:
            conn.execute("UPDATE todos SET done = 1 - done WHERE id=?", (todo_id,))
        if "title" in data:
            conn.execute("UPDATE todos SET title=? WHERE id=?", (data["title"], todo_id))
        if "category" in data:
            conn.execute("UPDATE todos SET category=? WHERE id=?", (data["category"], todo_id))
        if "priority" in data:
            conn.execute("UPDATE todos SET priority=? WHERE id=?", (data["priority"], todo_id))
        conn.commit()
        row = conn.execute("SELECT * FROM todos WHERE id=?", (todo_id,)).fetchone()
    if not row:
        return jsonify({"error": "not found"}), 404
    return jsonify(dict(row))


@app.route("/api/todos/<int:todo_id>", methods=["DELETE"])
def delete_todo(todo_id):
    with get_db() as conn:
        conn.execute("DELETE FROM todos WHERE id=?", (todo_id,))
        conn.commit()
    return jsonify({"ok": True})


# ── External Order API ─────────────────────────────────────────────────────────

def _load_cfg():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_cfg(cfg):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def _next_no(cfg):
    orders = cfg.get("external_orders", [])
    if not orders:
        return 1
    return max(int(o.get("no", 0)) for o in orders if isinstance(o, dict)) + 1


def _is_blank(val):
    return val in ("", "-", "--", ".")


def _parse_int(s):
    if _is_blank(s):
        return 0
    try:
        return int(re.sub(r"[,\s]", "", s))
    except ValueError:
        return None


VALID_ORD_TYPES = {
    "": "", "auto": "", "0": "",
    "05": "pre_market", "pre_market": "pre_market",
    "06": "post_market", "post_market": "post_market",
    "07": "overtime", "overtime": "overtime",
}


@app.route("/api/orders", methods=["GET"])
def get_orders():
    """현재 대기/처리 중인 외부주문 목록."""
    try:
        cfg = _load_cfg()
        orders = cfg.get("external_orders", [])
        return jsonify(orders)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/orders", methods=["POST"])
def add_order():
    """외부주문 등록. password 일치 필요."""
    data = request.json or {}

    # 비밀번호 확인
    if data.get("password") != ORDER_PASSWORD:
        return jsonify({"error": "비밀번호가 일치하지 않습니다"}), 403

    orders_input = data.get("orders", [])
    if not orders_input:
        return jsonify({"error": "주문 내용이 없습니다"}), 400

    try:
        cfg = _load_cfg()
        cur_no = _next_no(cfg)
        ext_orders = cfg.get("external_orders", [])
        if not isinstance(ext_orders, list):
            ext_orders = []

        added = []
        for o in orders_input:
            order_type = int(o.get("order", 0))
            if order_type not in (1, 2):
                continue
            symbol = str(o.get("symbol", "")).strip().upper()
            if not symbol:
                continue
            qty = int(o.get("qty", 0) or 0)
            amount = int(o.get("amount", 0) or 0)
            if qty <= 0 and amount <= 0:
                continue
            target_price = int(o.get("target_price", 0) or 0)
            target = str(o.get("target", "all")).strip() or "all"
            ord_type = str(o.get("ord_type", "")).strip()

            entry = {
                "no": cur_no,
                "order": order_type,
                "symbol": symbol,
                "qty": qty,
                "amount": amount,
                "target_price": target_price,
                "result": "",
                "target_qty": 0,
                "remain_qty": 0,
            }
            if ord_type and ord_type in VALID_ORD_TYPES:
                resolved = VALID_ORD_TYPES[ord_type]
                if resolved:
                    entry["ord_type"] = resolved
            if target != "all":
                entry["target"] = target

            ext_orders.append(entry)
            added.append(entry)
            cur_no += 1

        if not added:
            return jsonify({"error": "유효한 주문이 없습니다"}), 400

        cfg["external_orders"] = ext_orders
        _save_cfg(cfg)

        return jsonify({"ok": True, "count": len(added), "orders": added}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/orders/<int:no>", methods=["DELETE"])
def cancel_order(no):
    """대기 중인 외부주문 삭제 (비밀번호 불필요)."""
    try:
        cfg = _load_cfg()
        ext_orders = cfg.get("external_orders", [])
        found = [i for i, o in enumerate(ext_orders) if isinstance(o, dict) and o.get("no") == no]
        if not found:
            return jsonify({"error": f"#{no} 주문을 찾을 수 없습니다"}), 404
        removed = ext_orders.pop(found[0])
        if not ext_orders:
            cfg.pop("external_orders", None)
        else:
            cfg["external_orders"] = ext_orders
        _save_cfg(cfg)
        return jsonify({"ok": True, "removed": removed})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
