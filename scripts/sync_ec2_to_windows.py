#!/usr/bin/env python3
"""
EC2 → Windows 로컬 데이터 싱크 스크립트

- Windows PC에서 실행 (Python + paramiko 필요)
- EC2의 /home/ubuntu/Stoc_Kis/data 폴더들을 C:\\_DevProj\\_KIS_\\data 아래로 싱크
- part/, backup/ 하위 폴더는 싱크에서 제외
- 없는 파일은 복사, 있는 파일은 서버가 더 최신이면 교체 (mtime 비교)
- mtime은 epoch(UTC) 기준이라 서버/로컬 시간대가 달라도 비교에 문제 없음

싱크 대상:
  1. data/wss_data, 1m_data, 1d_data (part, backup 제외)
  2. symulation/Select_Tr_target_list.csv → data/target_list/
  3. data/admin/symbol_master/KRX_code.csv → 종목 기본코드/

Windows 작업 스케줄러로 매일 실행:
  - python sync_ec2_to_windows.py
  - 또는: py -3 "C:\\_DevProj\\_KIS_\\scripts\\sync_ec2_to_windows.py"
"""
from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path

# 설정 파일 경로 (스크립트와 같은 폴더 또는 상위 config)
SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / "sync_config.json"

# 기본 설정
DEFAULT_CONFIG = {
    "ssh_host": "your-ec2-host-or-ip",
    "ssh_port": 22,
    "ssh_user": "ubuntu",
    "ssh_key_path": "",  # 예: C:/Users/xxx/.ssh/ec2_key.pem
    "ssh_password": "",  # key 대신 비밀번호 사용 시
    "remote_base": "/home/ubuntu/Stoc_Kis",
    "local_base": r"C:\_DevProj\_KIS_",
    "exclude_dirs": ["parts", "part", "backup"],
}

REMOTE_DATA = "/home/ubuntu/Stoc_Kis/data"
REMOTE_SYMULATION = "/home/ubuntu/Stoc_Kis/symulation"

SYNC_ITEMS = [
    # (remote_path, local_path, is_dir)
    (f"{REMOTE_DATA}/wss_data", "data/wss_data", True),
    (f"{REMOTE_DATA}/1m_data", "data/1m_data", True),
    (f"{REMOTE_DATA}/1d_data", "data/1d_data", True),
    (f"{REMOTE_SYMULATION}/Select_Tr_target_list.csv", "data/target_list/Select_Tr_target_list.csv", False),
    (f"{REMOTE_DATA}/admin/symbol_master/KRX_code.csv", "종목 기본코드/KRX_code.csv", False),
]


def _load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, encoding="utf-8") as f:
            cfg = json.load(f)
        return {**DEFAULT_CONFIG, **cfg}
    return DEFAULT_CONFIG.copy()


def _save_default_config() -> None:
    if not CONFIG_PATH.exists():
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)
        print(f"[config] 기본 설정 저장: {CONFIG_PATH}")


def _should_exclude(rel_path: str, exclude_dirs: list[str]) -> bool:
    """경로에 제외할 폴더명이 포함되면 True"""
    parts = rel_path.replace("\\", "/").split("/")
    for p in parts:
        if p in exclude_dirs:
            return True
    return False


def sync_with_paramiko(config: dict) -> int:
    """paramiko SFTP로 싱크 실행. 복사한 파일 수 반환."""
    try:
        import paramiko
    except ImportError:
        print("[error] paramiko가 필요합니다: pip install paramiko")
        return 0

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    key_path = (config.get("ssh_key_path") or "").strip()
    password = config.get("ssh_password") or None
    port = int(config.get("ssh_port", 22))

    try:
        if key_path and os.path.exists(key_path):
            ssh.connect(
                config["ssh_host"],
                port=port,
                username=config["ssh_user"],
                key_filename=key_path,
                timeout=30,
            )
        else:
            if not password:
                print("[error] ssh_key_path 또는 ssh_password를 설정하세요. sync_config.json을 수정하세요.")
                return 0
            ssh.connect(
                config["ssh_host"],
                port=port,
                username=config["ssh_user"],
                password=password,
                timeout=30,
            )
    except Exception as e:
        print(f"[error] SSH 연결 실패: {e}")
        return 0

    sftp = ssh.open_sftp()
    exclude_dirs = config.get("exclude_dirs") or DEFAULT_CONFIG["exclude_dirs"]
    remote_base = config["remote_base"].rstrip("/")
    local_base = Path(config["local_base"].rstrip("/\\"))

    copied = 0

    def _walk_remote(rem_path: str, loc_path: Path, rel_sofar: str) -> None:
        nonlocal copied
        try:
            entries = sftp.listdir_attr(rem_path)
        except FileNotFoundError:
            return
        for e in entries:
            name = e.filename
            if name.startswith("."):
                continue
            rem_child = f"{rem_path}/{name}".replace("//", "/")
            rel_child = f"{rel_sofar}/{name}".replace("//", "/").strip("/")
            if _should_exclude(rel_child, exclude_dirs):
                continue
            loc_child = loc_path / name
            if e.st_mode is not None and stat.S_ISDIR(e.st_mode):
                loc_child.mkdir(parents=True, exist_ok=True)
                _walk_remote(rem_child, loc_child, rel_child)
            else:
                try:
                    remote_mtime = e.st_mtime
                except Exception:
                    remote_mtime = 0
                do_copy = False
                if not loc_child.exists():
                    do_copy = True
                else:
                    try:
                        local_mtime = loc_child.stat().st_mtime
                        if remote_mtime > local_mtime:
                            do_copy = True
                    except Exception:
                        do_copy = True
                if do_copy:
                    loc_child.parent.mkdir(parents=True, exist_ok=True)
                    sftp.get(rem_child, str(loc_child))
                    copied += 1
                    print(f"  [copy] {rel_child}")

    def _copy_file(rem_path: str, loc_path: Path) -> bool:
        nonlocal copied
        try:
            rstat = sftp.stat(rem_path)
        except FileNotFoundError:
            return False
        remote_mtime = rstat.st_mtime
        do_copy = False
        if not loc_path.exists():
            do_copy = True
        else:
            try:
                if remote_mtime > loc_path.stat().st_mtime:
                    do_copy = True
            except Exception:
                do_copy = True
        if do_copy:
            loc_path.parent.mkdir(parents=True, exist_ok=True)
            sftp.get(rem_path, str(loc_path))
            copied += 1
            print(f"  [copy] {loc_path.relative_to(local_base)}")
        return True

    for rem, loc, is_dir in SYNC_ITEMS:
        loc_full = local_base / loc.replace("/", os.sep)

        if is_dir:
            try:
                sftp.stat(rem)
            except FileNotFoundError:
                print(f"[skip] 원격 폴더 없음: {rem}")
                continue
            print(f"[sync] {rem} -> {loc_full}")
            _walk_remote(rem, loc_full, "")
        else:
            print(f"[sync] {rem} -> {loc_full}")
            if not _copy_file(rem, loc_full):
                print(f"  [skip] 원격 파일 없음: {rem}")

    sftp.close()
    ssh.close()
    return copied


def main() -> int:
    config = _load_config()
    if config["ssh_host"] == "your-ec2-host-or-ip":
        _save_default_config()
        print("[안내] sync_config.json을 생성했습니다.")
        print("       ssh_host, ssh_user, ssh_key_path(또는 ssh_password)를 수정한 뒤 다시 실행하세요.")
        return 1

    print("=" * 60)
    print("EC2 → Windows 데이터 싱크")
    print(f"  호스트: {config['ssh_host']}")
    print(f"  로컬: {config['local_base']}")
    print("  (mtime 기준: 서버가 더 최신일 때만 덮어씀)")
    print("=" * 60)

    n = sync_with_paramiko(config)
    print(f"\n[완료] {n}개 파일 동기화")
    return 0


if __name__ == "__main__":
    sys.exit(main())
