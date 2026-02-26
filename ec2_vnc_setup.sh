#!/bin/bash
# =============================================================
# EC2 Ubuntu 24.04 - VNC + XFCE + finplot 설치 스크립트
# =============================================================
# 실행: Cursor 터미널(EC2 원격 접속 상태)에서
#   chmod +x ~/ec2_vnc_setup.sh && ~/ec2_vnc_setup.sh
#
# VNC 접속 (설치 완료 후):
#   [방법 A] SSH 터널 (보안 권장) - 로컬 PC PowerShell에서:
#     ssh -i "your-key.pem" -L 5901:localhost:5901 ubuntu@<EC2-IP>
#     → VNC 클라이언트에서 localhost:5901 접속
#
#   [방법 B] 직접 접속 (EC2 보안그룹에서 5901 포트 오픈 필요):
#     → VNC 클라이언트에서 <EC2-IP>:5901 접속
#
# VNC 클라이언트 추천 (Windows):
#   - RealVNC Viewer (무료): https://www.realvnc.com/en/connect/download/viewer/
#   - TigerVNC Viewer: https://tigervnc.org/
#
# VNC 서버 관리:
#   시작:   vncserver :1
#   중지:   vncserver -kill :1
#   재시작: vncserver -kill :1 && vncserver :1
# =============================================================

set -e

echo "============================================"
echo " [1/5] 시스템 업데이트"
echo "============================================"
sudo apt update && sudo apt upgrade -y

echo "============================================"
echo " [2/5] XFCE 데스크톱 + TigerVNC 설치"
echo "============================================"
sudo apt install -y xfce4 xfce4-goodies tigervnc-standalone-server tigervnc-common dbus-x11

echo "============================================"
echo " [3/5] VNC 서버 설정"
echo "============================================"
mkdir -p ~/.vnc

cat > ~/.vnc/xstartup << 'XSTARTUP'
#!/bin/sh
unset SESSION_MANAGER
unset DBUS_SESSION_BUS_ADDRESS
export XDG_SESSION_TYPE=x11
exec startxfce4
XSTARTUP
chmod +x ~/.vnc/xstartup

cat > ~/.vnc/config << 'VNCCONFIG'
geometry=1920x1080
depth=24
VNCCONFIG

echo "============================================"
echo " [4/5] PyQt6 시스템 의존성 설치"
echo "============================================"
sudo apt install -y \
    python3-pip python3-venv \
    libgl1-mesa-glx libegl1 \
    libxcb-xinerama0 libxcb-cursor0 libxkbcommon0 \
    libdbus-1-3 libxcb-icccm4 libxcb-image0 libxcb-keysyms1 \
    libxcb-randr0 libxcb-render-util0 libxcb-shape0

echo "============================================"
echo " [5/5] Python 패키지 설치"
echo "============================================"
python3 -m venv ~/finplot_env
source ~/finplot_env/bin/activate
pip install --upgrade pip
pip install finplot pyqtgraph PyQt6 pandas numpy clipboard

echo ""
echo "============================================"
echo " 설치 완료!"
echo "============================================"
echo ""
echo " ▶ VNC 비밀번호 설정:  vncpasswd"
echo " ▶ VNC 서버 시작:     vncserver :1"
echo " ▶ 가상환경 활성화:   source ~/finplot_env/bin/activate"
echo ""
