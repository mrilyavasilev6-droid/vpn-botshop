import aiohttp
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class MarzbanAPI:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self._token = None
        self._token_expiry = None

    async def _login(self) -> str:
        """Получить JWT токен"""
        if self._token and self._token_expiry and datetime.now() < self._token_expiry:
            return self._token

        url = f"{self.base_url}/admin/token"
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data={
                "username": self.username,
                "password": self.password
            }) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self._token = data.get("access_token")
                    self._token_expiry = datetime.now() + timedelta(seconds=data.get("expires_in", 3600))
                    return self._token
                raise Exception(f"Marzban login failed: {resp.status}")

    async def _request(self, method: str, path: str, **kwargs) -> dict:
        token = await self._login()
        headers = {"Authorization": f"Bearer {token}"}
        if "headers" in kwargs:
            headers.update(kwargs["headers"])
        kwargs["headers"] = headers

        url = f"{self.base_url}{path}"
        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, **kwargs) as resp:
                if resp.status in (200, 201):
                    return await resp.json() if resp.content_type == "application/json" else {}
                raise Exception(f"Marzban API error {resp.status}: {await resp.text()}")

    async def create_user(self, username: str, expire_days: int, data_limit_gb: int = 0) -> dict:
        """Создать пользователя в Marzban"""
        expire = int((datetime.now() + timedelta(days=expire_days)).timestamp())
        
        payload = {
            "username": username,
            "proxies": {
                "vless": {},
                "vmess": {},
                "trojan": {},
                "shadowsocks": {}
            },
            "expire": expire,
            "data_limit": data_limit_gb * 1024 * 1024 * 1024 if data_limit_gb > 0 else 0,
            "status": "active",
            "inbounds": {
                "vless": ["VLESS TCP REALITY"],
                "vmess": ["VMESS TCP NOTLS"],
                "trojan": ["TROJAN TCP NOTLS"],
                "shadowsocks": ["Shadowsocks TCP"]
            }
        }
        
        return await self._request("POST", "/api/user", json=payload)

    async def get_user(self, username: str) -> Optional[dict]:
        """Получить информацию о пользователе"""
        try:
            return await self._request("GET", f"/api/user/{username}")
        except Exception:
            return None

    async def update_user_expiry(self, username: str, expire_days: int) -> dict:
        """Обновить срок действия пользователя"""
        user = await self.get_user(username)
        if not user:
            raise Exception(f"User {username} not found")
        
        new_expire = int((datetime.now() + timedelta(days=expire_days)).timestamp())
        user["expire"] = new_expire
        
        return await self._request("PUT", f"/api/user/{username}", json=user)

    async def delete_user(self, username: str) -> bool:
        """Удалить пользователя"""
        try:
            await self._request("DELETE", f"/api/user/{username}")
            return True
        except Exception:
            return False

    async def get_subscription_link(self, username: str) -> str:
        """Получить ссылку на подписку"""
        return f"{self.base_url}/sub/{username}"
