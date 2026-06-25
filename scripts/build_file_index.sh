#!/bin/bash
# ====================================================================
# build_file_index.sh
#  - /home/ubuntu 전체 트리의 파일 인덱스를 TSV 로 생성
#  - SSH_search_viewer 가 SFTP 로 받아서 즉시 검색에 사용
#  - venv / __pycache__ / .git / .cache 디렉토리 제외
#  - cron 1시간 주기 실행 권장: 0 * * * * /home/ubuntu/Stoc_Kis/scripts/build_file_index.sh
#
# 출력 포맷 (헤더 1줄 + 데이터 N줄):
#   #root=/home/ubuntu	ts=1715500800
#   size<TAB>mtime<TAB>fullpath
# 압축 결과 파일: /home/ubuntu/.cache/file_index.tsv.gz  (클라이언트는 이것을 받음)
# ====================================================================

set -euo pipefail

ROOT=/home/ubuntu
OUT=/home/ubuntu/.cache/file_index.tsv.gz
TMP=${OUT}.tmp

mkdir -p "$(dirname "$OUT")"
TS=$(date +%s)

{
  printf '#root=%s\tts=%s\n' "$ROOT" "$TS"
  find "$ROOT" \
    \( -path "$ROOT/.cursor-server" \
       -o -path "$ROOT/.vscode-server" \
       -o -path "$ROOT/.local" \
       -o -path "$ROOT/aws" \
       -o -path "$ROOT/.claude" \
       -o -path "$ROOT/.cursor" \
       -o -path "$ROOT/.npm" \
       -o -path "$ROOT/.duckdb" \
       -o -path "$ROOT/.dotnet" \
       -o -path "$ROOT/.config" \
       -o -type d \( -name venv -o -name __pycache__ -o -name .git -o -name .cache -o -name .pytest_cache -o -name node_modules \) \
    \) -prune \
    -o -type f -printf '%s\t%T@\t%p\n' 2>/dev/null
} | gzip -c > "$TMP"

mv "$TMP" "$OUT"
chmod 644 "$OUT"
