#!/usr/bin/env python3
"""restart_wss_DB1.sh 실행 (launch.json F5용)."""
import subprocess
import sys

if __name__ == "__main__":
    rc = subprocess.call(["/bin/bash", "/home/ubuntu/Stoc_Kis/restart_wss_DB1.sh"], cwd="/home/ubuntu/Stoc_Kis")
    sys.exit(rc)
