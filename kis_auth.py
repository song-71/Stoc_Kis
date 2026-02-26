"""
kis_auth.py
KIS API 인증 및 HTTP 요청 래퍼 모듈

bulk_trans_num.py 등에서 사용하는 인터페이스 제공
"""

import time
_t_import_start = time.perf_counter()

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional

import requests
from dateutil.parser import parse as dtparse

from kis_utils import load_config

print(f"[import] kis_auth module loaded in {(time.perf_counter() - _t_import_start):.3f}s")


# =============================================================================
# Constants
# =============================================================================

DEFAULT_BASE_URL = "https://openapi.koreainvestment.com:9443"


# =============================================================================
# Response Wrapper Classes
# =============================================================================

class KisResponse:
    """KIS API 응답 래퍼 클래스"""
    
    def __init__(self, response: requests.Response, tr_id: str):
        self.response = response
        self.tr_id = tr_id
        self._json = None
        self._body = None
        self._header = None
        
    def _get_json(self) -> dict:
        if self._json is None:
            try:
                self._json = self.response.json()
            except Exception:
                self._json = {}
        return self._json
    
    def isOK(self) -> bool:
        """응답이 성공인지 확인"""
        json_data = self._get_json()
        rt_cd = json_data.get("rt_cd", "")
        return str(rt_cd) == "0"
    
    def getErrorCode(self) -> str:
        """에러 코드 반환"""
        json_data = self._get_json()
        return str(json_data.get("rt_cd", ""))
    
    def getErrorMessage(self) -> str:
        """에러 메시지 반환"""
        json_data = self._get_json()
        return str(json_data.get("msg1", "") or json_data.get("msg_cd", ""))
    
    def getHeader(self) -> 'KisResponseHeader':
        """응답 헤더 래퍼 반환"""
        if self._header is None:
            json_data = self._get_json()
            self._header = KisResponseHeader(json_data.get("rt_cd", ""), json_data.get("msg1", ""))
        return self._header
    
    def getBody(self) -> 'KisResponseBody':
        """응답 본문 래퍼 반환"""
        if self._body is None:
            json_data = self._get_json()
            self._body = KisResponseBody(
                output=json_data.get("output", []),
                output2=json_data.get("output2", [])
            )
        return self._body
    
    def printError(self, api_url: str = "") -> None:
        """에러 정보 출력"""
        print(f"[KIS Error] URL: {api_url}")
        print(f"  Code: {self.getErrorCode()}")
        print(f"  Message: {self.getErrorMessage()}")
        if self.response.status_code != 200:
            print(f"  HTTP Status: {self.response.status_code}")


class KisResponseHeader:
    """응답 헤더 래퍼"""
    
    def __init__(self, rt_cd: str, msg1: str):
        self.rt_cd = rt_cd
        self.msg1 = msg1
        # 연속 거래 여부 (tr_cont: "M" = 연속 있음, "F" = 연속 없음)
        self.tr_cont = "F"  # 기본값, 실제로는 응답에서 가져와야 함


class KisResponseBody:
    """응답 본문 래퍼"""
    
    def __init__(self, output: list, output2: list):
        self.output = output if isinstance(output, list) else []
        self.output2 = output2 if isinstance(output2, list) else []
        # output이 단일 딕셔너리인 경우 리스트로 변환
        if isinstance(output, dict):
            self.output = [output]


# =============================================================================
# Config / Client
# =============================================================================

@dataclass
class KisConfig:
    appkey: str
    appsecret: str
    base_url: str = DEFAULT_BASE_URL
    custtype: str = "P"
    token_cache_path: str = "./kis_token.json"
    timeout_sec: int = 10
    max_http_retries: int = 4
    http_backoff_sec: float = 0.7


class KisAuthClient:
    """KIS API 인증 및 HTTP 요청 클라이언트"""
    
    _instance: Optional['KisAuthClient'] = None
    _config: Optional[KisConfig] = None
    
    def __init__(self, cfg: Optional[KisConfig] = None):
        if cfg is None:
            # config.json에서 자동 로드
            try:
                config_path = os.path.join(os.path.dirname(__file__), "config.json")
                cfg_json = load_config(config_path)
                cfg = KisConfig(
                    appkey=cfg_json.get("appkey", ""),
                    appsecret=cfg_json.get("appsecret", ""),
                    base_url=cfg_json.get("base_url", DEFAULT_BASE_URL),
                    token_cache_path=cfg_json.get("token_cache_path", "./kis_token.json")
                )
            except Exception as e:
                raise ValueError(f"config.json 로드 실패: {e}. appkey/appsecret를 config.json에 설정하세요.")
        
        self.cfg = cfg
        self.session = requests.Session()
        self.access_token: Optional[str] = None
        self.token_expire_dt: Optional[datetime] = None
    
    @classmethod
    def get_instance(cls, cfg: Optional[KisConfig] = None) -> 'KisAuthClient':
        """싱글톤 인스턴스 반환"""
        if cls._instance is None or cfg is not None:
            cls._instance = cls(cfg)
        return cls._instance
    
    def _load_cached_token(self) -> bool:
        """캐시된 토큰 로드"""
        path = self.cfg.token_cache_path
        if not os.path.exists(path):
            return False
        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)
            token = obj.get("access_token")
            exp = obj.get("access_token_token_expired")
            if not token or not exp:
                return False
            exp_dt = dtparse(exp)
            if exp_dt <= datetime.now() + timedelta(minutes=5):
                return False
            self.access_token = token
            self.token_expire_dt = exp_dt
            return True
        except Exception:
            return False
    
    def _save_token(self, resp_json: dict) -> None:
        """토큰 저장"""
        os.makedirs(os.path.dirname(self.cfg.token_cache_path) or ".", exist_ok=True)
        with open(self.cfg.token_cache_path, "w", encoding="utf-8") as f:
            json.dump(resp_json, f, ensure_ascii=False, indent=2)
    
    def ensure_token(self) -> str:
        """토큰 확보 (캐시 우선, 필요시 새로 발급)"""
        if self.access_token and self.token_expire_dt:
            if self.token_expire_dt > datetime.now() + timedelta(minutes=5):
                return self.access_token
        
        if self._load_cached_token():
            return self.access_token  # type: ignore
        
        url = f"{self.cfg.base_url}/oauth2/token"
        data = {
            "grant_type": "client_credentials",
            "appkey": self.cfg.appkey,
            "appsecret": self.cfg.appsecret,
        }
        
        last_err = None
        for attempt in range(1, self.cfg.max_http_retries + 1):
            try:
                r = self.session.post(url, data=data, timeout=self.cfg.timeout_sec)
                r.raise_for_status()
                j = r.json()
                if "access_token" not in j:
                    raise RuntimeError(f"Token 발급 실패: {j}")
                
                expires_in = int(j.get("expires_in", 3600))
                self.access_token = j["access_token"]
                self.token_expire_dt = datetime.now() + timedelta(seconds=expires_in)
                j.setdefault("access_token_token_expired", self.token_expire_dt.strftime("%Y-%m-%d %H:%M:%S"))
                self._save_token(j)
                return self.access_token
            except Exception as e:
                last_err = e
                sleep = self.cfg.http_backoff_sec * (2 ** (attempt - 1))
                print(f"[KIS][token] attempt={attempt}/{self.cfg.max_http_retries} failed: {e} (sleep {sleep:.1f}s)")
                time.sleep(sleep)
        
        raise RuntimeError(f"Token 발급 최종 실패: {last_err}")
    
    def _url_fetch(self, api_url: str, tr_id: str, tr_cont: str, params: dict) -> KisResponse:
        """
        KIS API 호출 (GET 요청)
        
        Args:
            api_url: API 경로 (예: "/uapi/domestic-stock/v1/ranking/bulk-trans-num")
            tr_id: 거래 ID (예: "FHKST190900C0")
            tr_cont: 연속 거래 여부 ("M" = 연속, "F" = 처음, "N" = 다음)
            params: 요청 파라미터 딕셔너리
        
        Returns:
            KisResponse: 응답 래퍼 객체
        """
        token = self.ensure_token()
        url = f"{self.cfg.base_url}{api_url}"
        
        headers = {
            "authorization": f"Bearer {token}",
            "appkey": self.cfg.appkey,
            "appsecret": self.cfg.appsecret,
            "tr_id": tr_id,
            "custtype": self.cfg.custtype,
        }
        
        # tr_cont가 있으면 헤더에 추가
        if tr_cont:
            headers["tr_cont"] = tr_cont
        
        last_err = None
        for attempt in range(1, self.cfg.max_http_retries + 1):
            try:
                r = self.session.get(url, headers=headers, params=params, timeout=self.cfg.timeout_sec)
                r.raise_for_status()
                
                # KisResponse 생성 및 tr_cont 파싱
                resp = KisResponse(r, tr_id)
                json_data = resp._get_json()
                
                # tr_cont를 헤더에서 읽어서 설정
                if "tr_cont" in json_data:
                    resp.getHeader().tr_cont = str(json_data["tr_cont"])
                
                return resp
            except Exception as e:
                last_err = e
                sleep = self.cfg.http_backoff_sec * (2 ** (attempt - 1))
                print(f"[KIS][fetch] {api_url} attempt={attempt}/{self.cfg.max_http_retries} failed: {e} (sleep {sleep:.1f}s)")
                time.sleep(sleep)
        
        # 최종 실패 시 빈 응답 반환 (에러는 KisResponse에서 확인 가능)
        try:
            r = requests.Response()
            r.status_code = 500
            resp = KisResponse(r, tr_id)
            resp._json = {"rt_cd": "500", "msg1": str(last_err)}
            return resp
        except Exception:
            raise RuntimeError(f"KIS API 호출 최종 실패: {last_err}")


# =============================================================================
# Module-level functions (bulk_trans_num.py 호환 인터페이스)
# =============================================================================

_client: Optional[KisAuthClient] = None


def _get_client() -> KisAuthClient:
    """싱글톤 클라이언트 반환"""
    global _client
    if _client is None:
        _client = KisAuthClient.get_instance()
    return _client


def _url_fetch(api_url: str, tr_id: str, tr_cont: str, params: dict) -> KisResponse:
    """모듈 레벨 함수: KIS API 호출"""
    return _get_client()._url_fetch(api_url, tr_id, tr_cont, params)


def smart_sleep(seconds: float = 0.25) -> None:
    """
    API 호출 간 지연 (KIS API 레이트 리밋 준수)
    
    Args:
        seconds: 대기 시간 (초), 기본값 0.25초
    """
    time.sleep(seconds)


# =============================================================================
# Export
# =============================================================================

__all__ = [
    "KisConfig",
    "KisAuthClient",
    "KisResponse",
    "KisResponseHeader",
    "KisResponseBody",
    "_url_fetch",
    "smart_sleep",
]
