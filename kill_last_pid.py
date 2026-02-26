import os
import signal
import time

from kis_utils import load_config, save_config


def main() -> None:
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    cfg = load_config(config_path)
    pid_val = cfg.get("last_pid")
    if not pid_val:
        print("[kill] config에 last_pid가 없습니다.")
        return

    try:
        pid = int(str(pid_val))
    except Exception:
        print(f"[kill] last_pid 파싱 실패: {pid_val}")
        return

    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        print(f"[kill] 프로세스 없음 pid={pid}")
    except PermissionError as e:
        print(f"[kill] 권한 오류 pid={pid} error={e}")
        return
    except Exception as e:
        print(f"[kill] 종료 실패 pid={pid} error={e}")
        return

    # 종료 확인(최대 5초)
    for _ in range(10):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            break
        except Exception:
            break
        time.sleep(0.5)

    # 종료되었으면 config에서 제거
    try:
        os.kill(pid, 0)
        print(f"[kill] 종료 확인 실패(살아있음) pid={pid}")
        return
    except ProcessLookupError:
        pass

    cfg.pop("last_pid", None)
    cfg.pop("last_pid_time", None)
    save_config(cfg, config_path)
    print(f"[kill] 종료 완료 및 config에서 last_pid 제거 pid={pid}")


if __name__ == "__main__":
    main()
