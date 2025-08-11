from typing import Dict, Optional

import httpx

from .logging import get_logger
from .retry import create_http_retry_config, retry_with_backoff

logger = get_logger(__name__)


class GiteaProvider:
    def __init__(
        self,
        user: str,
        url: Optional[str] = None,
        token: Optional[str] = None,
        default_org_email: Optional[str] = None,
        default_location: Optional[str] = None,
    ):
        self.user = user
        self.url = url
        self.token = token
        self.default_org_email = default_org_email
        self.default_location = default_location
        self._client = None

    def _get_headers(self) -> Dict[str, str]:
        """获取通用请求头"""
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    async def _get_client(self) -> httpx.AsyncClient:
        """获取或创建HTTP客户端"""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=30.0,
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
            )
        return self._client

    async def close(self):
        """关闭HTTP客户端"""
        if self._client:
            await self._client.aclose()
            self._client = None

    @retry_with_backoff(create_http_retry_config())
    async def create_org(self, org_name: str):
        """
        在 Gitea 上创建一个新的组织
        :param org_name: 组织名称
        """
        # check if org already exists
        org_data = await self.get_org(org_name)
        if org_data:
            return org_data

        # create org
        api_url = f"{self.url}/api/v1/orgs"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        data = {
            "description": f"organization for data service {org_name}",
            "email": self.default_org_email,
            "full_name": f"Data Service {org_name}",
            "location": self.default_location,
            "repo_admin_change_team_access": True,
            "username": org_name,
            "visibility": "public",
        }
        try:
            client = await self._get_client()
            response = await client.post(
                api_url, headers=self._get_headers(), json=data
            )
            response.raise_for_status()
            org_data = response.json()
            logger.info(
                "Created organization successfully",
                action="create_org",
                org_name=org_name,
            )
            return org_data
        except httpx.HTTPStatusError as e:
            logger.error(
                "Failed to create organization",
                action="create_org",
                org_name=org_name,
                status_code=e.response.status_code,
                error=str(e),
            )
            raise Exception(f"创建组织失败: {e}")

    @retry_with_backoff(create_http_retry_config())
    async def get_org(self, org_name: str):
        """
        获取 Gitea 上的组织信息
        :param org_name: 组织名称
        """
        api_url = f"{self.url}/api/v1/orgs/{org_name}"
        try:
            client = await self._get_client()
            response = await client.get(api_url, headers=self._get_headers())
            if response.status_code == 404:
                return None
            response.raise_for_status()
            org_data = response.json()
            return org_data
        except httpx.HTTPStatusError as e:
            logger.error(
                "Failed to get organization",
                action="get_org",
                org_name=org_name,
                status_code=e.response.status_code,
                error=str(e),
            )
            raise Exception(f"获取组织信息失败: {e}")

    @retry_with_backoff(create_http_retry_config())
    async def create_repo(self, org_name: str, repo_name: str):
        """
        在 Gitea 上创建一个新的远程仓库
        :param org_name: 组织名称
        :param repo_name: 仓库名称
        """
        api_url = f"{self.url}/api/v1/orgs/{org_name}/repos"
        data = {
            "name": repo_name,
            "description": f"repository for data service {repo_name}",
            "private": True,
            "default_branch": "main",
        }
        try:
            client = await self._get_client()
            response = await client.post(
                api_url, headers=self._get_headers(), json=data
            )
            response.raise_for_status()
            repo_data = response.json()
            logger.info(
                "Created repository successfully",
                action="create_repo",
                org_name=org_name,
                repo_name=repo_name,
            )
            return repo_data
        except httpx.HTTPStatusError as e:
            logger.error(
                "Failed to create repository",
                action="create_repo",
                org_name=org_name,
                repo_name=repo_name,
                status_code=e.response.status_code,
                error=str(e),
            )
            raise Exception(f"创建仓库失败: {e}")

    @retry_with_backoff(create_http_retry_config())
    async def get_repo(self, org_name: str, repo_name: str):
        """
        获取 Gitea 上的仓库信息
        :param org_name: 组织名称
        :param repo_name: 仓库名称
        """
        api_url = f"{self.url}/api/v1/repos/{org_name}/{repo_name}"
        try:
            client = await self._get_client()
            response = await client.get(api_url, headers=self._get_headers())
            if response.status_code == 404:
                return None
            response.raise_for_status()
            repo_data = response.json()
            return repo_data
        except httpx.HTTPStatusError as e:
            logger.error(
                "Failed to get repository",
                action="get_repo",
                org_name=org_name,
                repo_name=repo_name,
                status_code=e.response.status_code,
                error=str(e),
            )
            raise Exception(f"获取仓库信息失败: {e}")

    @retry_with_backoff(create_http_retry_config())
    async def delete_repo(self, org_name: str, repo_name: str):
        """
        删除 Gitea 上的仓库
        :param org_name: 组织名称
        :param repo_name: 仓库名称
        """
        api_url = f"{self.url}/api/v1/repos/{org_name}/{repo_name}"
        try:
            client = await self._get_client()
            response = await client.delete(api_url, headers=self._get_headers())
            response.raise_for_status()
            logger.info(
                "Deleted repository successfully",
                action="delete_repo",
                org_name=org_name,
                repo_name=repo_name,
            )
            return True
        except httpx.HTTPStatusError as e:
            logger.error(
                "Failed to delete repository",
                action="delete_repo",
                org_name=org_name,
                repo_name=repo_name,
                status_code=e.response.status_code,
                error=str(e),
            )
            raise Exception(f"删除仓库失败: {e}")

    @retry_with_backoff(create_http_retry_config())
    async def list_org_repos(self, org_name: str, page: int = 1, limit: int = 50):
        """
        获取组织下的所有仓库列表
        :param org_name: 组织名称
        :param page: 页码
        :param limit: 每页数量
        """
        if page < 1:
            raise ValueError("Page must be greater than 0")
        if limit < 1 or limit > 100:
            raise ValueError("Limit must be between 1 and 100")

        api_url = f"{self.url}/api/v1/orgs/{org_name}/repos"
        params = {"page": page, "limit": limit}
        try:
            client = await self._get_client()
            response = await client.get(
                api_url, headers=self._get_headers(), params=params
            )
            response.raise_for_status()
            repos_data = response.json()
            return repos_data
        except httpx.HTTPStatusError as e:
            logger.error(
                "Failed to list organization repositories",
                action="list_org_repos",
                org_name=org_name,
                status_code=e.response.status_code,
                error=str(e),
            )
            raise Exception(f"获取仓库列表失败: {e}")
