import httpx


class GiteaProvider:
    def __init__(self, user: str, url: str, token: str):
        self.user = user
        self.url = url
        self.token = token

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
            "email": "data-service@example.com",
            "full_name": f"Data Service {org_name}",
            "location": "China",
            "repo_admin_change_team_access": True,
            "username": org_name,
            "visibility": "public",
        }
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(api_url, headers=headers, json=data)
            response.raise_for_status()
            org_data = response.json()
            return org_data
        except httpx.HTTPStatusError as e:
            raise Exception(f"创建组织失败: {e}")

    async def get_org(self, org_name: str):
        """
        获取 Gitea 上的组织信息
        :param org_name: 组织名称
        """
        api_url = f"{self.url}/api/v1/orgs/{org_name}"
        headers = {
            "Authorization": f"Bearer {self.token}",
        }
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(api_url, headers=headers)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            org_data = response.json()
            return org_data
        except httpx.HTTPStatusError as e:
            raise Exception(f"获取组织信息失败: {e}")

    async def create_repo(self, org_name: str, repo_name: str):
        """
        在 Gitea 上创建一个新的远程仓库
        :param org_name: 组织名称
        :param repo_name: 仓库名称
        """
        api_url = f"{self.url}/api/v1/orgs/{org_name}/repos"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        data = {
            "name": repo_name,
            "description": f"repository for data service {repo_name}",
            "private": True,
            "default_branch": "main",
        }
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(api_url, headers=headers, json=data)
            response.raise_for_status()
            repo_data = response.json()
            return repo_data
        except httpx.HTTPStatusError as e:
            raise Exception(f"创建仓库失败: {e}")

    async def get_repo(self, org_name: str, repo_name: str):
        """
        获取 Gitea 上的仓库信息
        :param org_name: 组织名称
        :param repo_name: 仓库名称
        """
        api_url = f"{self.url}/api/v1/repos/{org_name}/{repo_name}"
        headers = {
            "Authorization": f"Bearer {self.token}",
        }
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(api_url, headers=headers)
            # 检查 HTTP 状态码
            if response.status_code == 404:
                return None  # 仓库不存在
            response.raise_for_status()
            repo_data = response.json()
            return repo_data
        except httpx.HTTPStatusError as e:
            raise Exception(f"获取仓库信息失败: {e}")

    async def delete_repo(self, org_name: str, repo_name: str):
        """
        删除 Gitea 上的仓库
        :param org_name: 组织名称
        :param repo_name: 仓库名称
        """
        api_url = f"{self.url}/api/v1/repos/{org_name}/{repo_name}"
        headers = {
            "Authorization": f"Bearer {self.token}",
        }
        try:
            async with httpx.AsyncClient() as client:
                response = await client.delete(api_url, headers=headers)
            response.raise_for_status()
            return True
        except httpx.HTTPStatusError as e:
            raise Exception(f"删除仓库失败: {e}")

    async def list_org_repos(self, org_name: str, page: int = 1, limit: int = 50):
        """
        获取组织下的所有仓库列表
        :param org_name: 组织名称
        :param page: 页码
        :param limit: 每页数量
        """
        api_url = f"{self.url}/api/v1/orgs/{org_name}/repos"
        headers = {
            "Authorization": f"Bearer {self.token}",
        }
        params = {"page": page, "limit": limit}
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(api_url, headers=headers, params=params)
            response.raise_for_status()
            repos_data = response.json()
            return repos_data
        except httpx.HTTPStatusError as e:
            raise Exception(f"获取仓库列表失败: {e}")
