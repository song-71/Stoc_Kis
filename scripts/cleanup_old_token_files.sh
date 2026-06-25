#!/bin/bash
# 260604 a1/a2 마이그레이션 후속: 구 토큰 캐시 파일 1회성 정리.
#  - 배경: 계좌 식별자/토큰 캐시를 a1/a2 체계로 통일(kis_token_a1.json/kis_token_a2.json).
#    구 파일(kis_token.json, kis_token_main.json, kis_token_syw2.json, kis_token_syw_2.json)은
#    당시 실행 중이던 watchdog 등이 참조 중이라 즉시 삭제하지 않고 남겨둠.
#  - 이 스크립트: 내일 아침(08:20~08:58 KST) 전체 cron 재시작으로 모두 새 코드(a1/a2)로 전환된 뒤,
#    조건 충족 시에만 구 파일을 삭제하고 cron 라인을 자기 제거(1회성).
DIR=/home/ubuntu/Stoc_Kis
LOG="$DIR/out/logs/token_cleanup_260605.log"
SENTINEL="$DIR/.token_cleanup_done"
ts() { TZ=Asia/Seoul date '+%Y-%m-%d %H:%M:%S KST'; }
selfremove() { crontab -l 2>/dev/null | grep -v 'cleanup_old_token_files.sh' | crontab -; }

# 이미 완료됐으면 cron 라인만 정리하고 종료
[ -f "$SENTINEL" ] && { selfremove; exit 0; }

# 조건1: 프로덕션(새 코드)이 떠 있는지 = 아침 재시작 완료 확인
if ! pgrep -f 'ws_realtime_trading.py' >/dev/null 2>&1; then
  echo "$(ts) [skip] ws_realtime_trading 미실행 — 재시작 미완료로 정리 보류" >>"$LOG"
  exit 0
fi

# 조건2: 새 a1/a2 토큰 파일이 존재하고 비어있지 않은지
for f in kis_token_a1.json kis_token_a2.json; do
  if [ ! -s "$DIR/$f" ]; then
    echo "$(ts) [skip] $f 없음/빈파일 — 정리 보류" >>"$LOG"
    exit 0
  fi
done

# 조건 충족 → 구 파일 삭제
cd "$DIR" || exit 0
deleted=""
for old in kis_token.json kis_token_main.json kis_token_syw2.json kis_token_syw_2.json; do
  if [ -f "$old" ]; then
    rm -f "$old" && deleted="$deleted $old"
  fi
done
touch "$SENTINEL"
echo "$(ts) [done] 구 토큰 파일 정리 완료. 삭제:${deleted:- (이미 없음)}" >>"$LOG"
selfremove
