# strategy_lab

KIS 공식 GitHub (open-trading-api) 의 Strategy Builder + Backtester 를 흡수해서
자연어 기반 전략 설계 / 백테스트를 진행하는 작업장입니다.

기존 실시간 운영 코드 (`ws_realtime_trading.py`, `ws_realtime_tr_str1.py`) 와는
**입력 데이터만 공유**하고 코드 의존은 단방향입니다 (strategy_lab → ws_realtime_trading 으로만,
역방향 금지).

## 폴더 구조

```
strategy_lab/
├── open_trading_api/      git subtree (koreainvestment/open-trading-api, read-only)
│   ├── strategy_builder/  지표 80+, 캔들패턴 66, BaseStrategy, YAML DSL
│   ├── backtester/        QuantConnect Lean 기반 Docker 백테스터 + Python wrapper
│   ├── examples_user/     REST/WS 카테고리별 단건 호출 샘플
│   ├── examples_llm/      LLM 친화 형태 단건 샘플
│   └── MCP/               MCP 서버 (kis-ai-extensions 와 연동)
├── strategies_local/      우리가 작성하는 .kis.yaml + BaseStrategy 파생 (gitignore 제외)
├── data_adapters/         우리 일봉/1분봉/KRX/VI/상한가 DB → strategy_builder 입력 어댑터
├── backtests/
│   ├── configs/           백테스트 설정 (자본금, 수수료, 기간, universe)
│   ├── results/           json 결과 (gitignore)
│   └── reports/           HTML 리포트 (gitignore)
└── tests/                 어댑터 단위테스트
```

## 운영 코드와의 경계

| 항목 | strategy_lab | ws_realtime_trading.py |
|---|---|---|
| 전략 설계 / 백테스트 | O | X |
| 지표 / 캔들 패턴 / DSL | O | X |
| 실시간 WSS 체결 / 매수 / 매도 | X | O |
| VI 매매 / 멀티계좌 / 텔레그램 | X | O |
| 후보 종목 산출 | O (배치) | X (소비만) |

후보 종목은 cron 기반으로 매일 갱신되며,
`data/fetch_top_list/*_candidates_{yymmdd}.json` 형태로 ws_realtime_trading 에 전달됩니다.

## 설치 (Phase 1 에서 진행)

```bash
# 1. 의존성 분리된 venv
cd strategy_lab
python -m venv .venv && source .venv/bin/activate
pip install -r open_trading_api/requirements.txt
pip install -r requirements.txt  # 우리 추가 의존성 (Phase 1 에서 채움)

# 2. kis-ai-extensions skill 설치 (자연어 인터페이스)
# - .claude/skills/ 에 kis-strategy-builder, kis-backtester 등 5종 설치
# - MCP 서버 127.0.0.1:3846/mcp 기동
```

## 사용 흐름 (Phase 1 완료 후)

1. Claude Code 에서 자연어로 전략 설계 → kis-strategy-builder skill
   → `strategies_local/<name>.kis.yaml` 자동 생성
2. 백테스트 실행 → kis-backtester skill / MCP run_backtest
   → `backtests/results/<name>.json` + `backtests/reports/<name>.html`
3. 파라미터 스윕 → MCP optimize_params
4. 검증 통과한 전략만 사용자 승인 후 cron + bridge.py 로 운영 연결

## 외부 코드 갱신

```bash
git subtree pull --prefix=strategy_lab/open_trading_api \
  https://github.com/koreainvestment/open-trading-api.git main --squash
```
