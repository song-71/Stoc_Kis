# EC2 → Windows 데이터 싱크

## 개요

`sync_ec2_to_windows.py`는 EC2 서버의 데이터를 Windows 로컬로 동기화합니다.

- **실행 위치**: Windows PC (Python + paramiko 필요)
- **동기화 기준**: 없는 파일은 복사, 있는 파일은 서버 mtime이 더 최신일 때만 교체
- **시간대**: mtime은 epoch(UTC)라 서버/로컬 시간대가 달라도 비교에 문제 없음

## 싱크 대상

| 원격 (EC2) | 로컬 (Windows) |
|------------|----------------|
| data/wss_data (part, backup 제외) | C:\_DevProj\_KIS_\data\wss_data |
| data/1m_data | C:\_DevProj\_KIS_\data\1m_data |
| data/1d_data | C:\_DevProj\_KIS_\data\1d_data |
| symulation/Select_Tr_target_list.csv | data\target_list\Select_Tr_target_list.csv |
| data/admin/symbol_master/KRX_code.csv | 종목 기본코드\KRX_code.csv |

## 설치

```bash
pip install paramiko
```

## 설정

1. 첫 실행 시 `sync_config.json`이 자동 생성됩니다.
2. 다음 항목을 수정하세요:
   - `ssh_host`: EC2 호스트명 또는 IP
   - `ssh_user`: SSH 사용자 (예: ubuntu)
   - `ssh_key_path`: PEM 키 경로 (예: `C:/Users/xxx/.ssh/ec2_key.pem`)
   - 또는 `ssh_password`: 비밀번호 (키 대신 사용 시)

## 실행

```cmd
python sync_ec2_to_windows.py
```

또는 (스크립트를 EC2에서 Windows로 복사한 경우):

```cmd
py -3 "C:\_DevProj\_KIS_\scripts\sync_ec2_to_windows.py"
```

## Windows 작업 스케줄러 등록

1. `작업 스케줄러` → `기본 작업 만들기`
2. 트리거: 매일 원하는 시간 (예: 새벽 6시)
3. 동작: `프로그램 시작`
   - 프로그램: `python` 또는 `py`
   - 인수: `-3 "C:\_DevProj\_KIS_\scripts\sync_ec2_to_windows.py"`
   - 시작 위치: `C:\_DevProj\_KIS_\scripts`
4. `가장 높은 권한으로 실행` 체크 해제 (필요 시)
5. `사용자가 로그온할 때만 실행` 선택 시, 로그인 상태여야 함. `사용자의 로그온 여부에 관계없이 실행`이 더 안정적
