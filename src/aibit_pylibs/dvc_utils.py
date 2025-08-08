import asyncio
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from dvc.api import DVCFileSystem
from dvc.repo import Repo as DvcRepo
from git import Repo as GitRepo

from .file_util import (
    DirectoryContent,
    FileItem,
    FileTree,
    FileTreeNode,
    FileUtils,
    VersionFileList,
)
from .git_utils import GitUtils
from .gitea_provider import GiteaProvider
from .logging import get_logger

logger = get_logger(__name__)

dataset_readme_template = """
# {dataset_name}
数据集 ID: {dataset_id}
租户: {tenant_name}
文件数量: {total_files}
总大小: {total_size:,} bytes

## 使用方法

```bash
# 拉取数据
dvc pull

# 查看数据
ls data/
```

## 版本历史

- v1.0: 初始版本，来源于上传文件 {Path(file_path).name}
"""


class DvcUtils:
    def __init__(
        self,
        gitea_provider: GiteaProvider,
        s3_endpoint_url: str,
        s3_access_key_id: str,
        s3_secret_access_key: str,
    ):
        self.gitea_provider = gitea_provider
        self.s3_endpoint_url = s3_endpoint_url
        self.s3_access_key_id = s3_access_key_id
        self.s3_secret_access_key = s3_secret_access_key

    async def initialize_dataset_repo(
        self,
        task_id: str,
        dataset_id: str,
        dataset_name: str,
        dataset_prefix_path: str,
        tenant_name: str,
        file_path: str,
    ) -> Dict[str, Any]:
        """
        初始化数据集Git仓库和DVC配置，解压用户上传的文件并管理
        :param task_id: 任务ID
        :param dataset_id: 数据集ID
        :param dataset_name: 数据集名称
        :param dataset_prefix_path: 数据集路径前缀
        :param tenant_name: 租户名称
        :param file_path: 用户上传的压缩文件路径
        """

        task_logger = logger.bind(
            task_id=task_id,
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            tenant_name=tenant_name,
        )

        try:
            # 创建临时目录进行所有操作
            with tempfile.TemporaryDirectory() as temp_base_dir:
                temp_base = Path(temp_base_dir)

                # 1. 解压用户上传的文件到临时目录
                extract_dir = Path(f"{temp_base}/{dataset_id}/data")
                FileUtils.extract_file(file_path, extract_dir)

                # 计算解压后的文件统计信息
                total_files = FileUtils.count_files_in_directory(extract_dir)
                total_size = FileUtils.calculate_directory_size(extract_dir)

                # 2. 设置Git仓库
                repo_dir = Path(f"{temp_base}/{dataset_id}")

                # 创建Git仓库
                git_utils = GitUtils(str(repo_dir))

                # 创建或获取组织
                await self.gitea_provider.create_org(tenant_name)

                # 创建或获取仓库
                repo_info = await self.gitea_provider.get_repo(tenant_name, dataset_id)

                if repo_info:
                    repo_url = repo_info["clone_url"]
                else:
                    repo_data = await self.gitea_provider.create_repo(
                        tenant_name, dataset_id
                    )
                    repo_url = repo_data["clone_url"]

                # 判断repo_url是http还是https，并相应地插入用户名和token
                if repo_url.startswith("https://"):
                    repo_url_with_token = repo_url.replace(
                        "https://",
                        f"https://{self.gitea_provider.user}:{self.gitea_provider.token}@",
                    )
                elif repo_url.startswith("http://"):
                    repo_url_with_token = repo_url.replace(
                        "http://",
                        f"http://{self.gitea_provider.user}:{self.gitea_provider.token}@",
                    )
                else:
                    # 如果不是http/https，直接使用原始repo_url
                    repo_url_with_token = repo_url

                # 4. 配置远程仓库
                git_utils.add_remote("origin", repo_url_with_token)

                # 5. 初始化DVC
                dvc_repo = DvcRepo.init(repo_dir, no_scm=True)

                # 6. 配置DVC远程存储
                dvc_remote_url = f"s3://{dataset_prefix_path}/{dataset_id}"
                with dvc_repo.config.edit() as config:
                    # 设置默认远程存储
                    config["remote"]["s3_storage"] = {
                        "url": dvc_remote_url,
                        "endpointurl": self.s3_endpoint_url,
                        "access_key_id": self.s3_access_key_id,
                        "secret_access_key": self.s3_secret_access_key,
                    }
                    config["core"]["remote"] = "s3_storage"

                # 8. 将data目录添加到DVC跟踪
                dvc_repo.add(str(extract_dir))

                readme_content = dataset_readme_template.format(
                    dataset_name=dataset_name,
                    dataset_id=dataset_id,
                    tenant_name=tenant_name,
                    total_files=total_files,
                    total_size=total_size,
                    file_path=file_path,
                )

                # 9. 创建README文件
                (repo_dir / "README.md").write_text(readme_content, encoding="utf-8")

                # 10. 提交初始版本到Git
                commit_hash = git_utils.add_dvc_and_commit(
                    f"初始化数据集 {dataset_name} - 来源文件: {Path(file_path).name}"
                )

                # 11. 创建初始版本v1标签
                git_utils.create_tag(
                    "v1", f"初始版本 - {total_files}个文件，{total_size:,}字节"
                )

                # 12. 推送到Git+DVC远程仓库
                dvc_repo.push()
                git_utils.push("origin", "main")
                git_utils.push_tag("v1", "origin")

                return {
                    "dataset_id": dataset_id,
                    "git_repo_url": repo_url,
                    "commit_hash": commit_hash,
                    "file_count": total_files,
                    "total_size": total_size,
                    "version_tag": "v1",
                    "status": "initialized",
                }

        except Exception as e:
            task_logger.error(
                "Dataset initialization task failed",
                action="initialize_dataset_task",
                error=str(e),
                exc_info=True,
            )
            raise Exception(f"初始化数据集任务失败: {e}")

    async def create_dataset_version(
        self,
        task_id: str,
        dataset_id: int,
        dataset_git_url: str,
        version_id: int,
        version_tag: str,
        file_references: List[FileItem],
        commit_message: str,
    ) -> Dict[str, Any]:
        """处理版本创建的异步任务（基于文件引用）"""

        task_logger = logger.bind(task_id=task_id, dataset_id=dataset_id)

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                repo_path = Path(temp_dir) / "repo"

                # 1. 克隆仓库
                repo = GitRepo.clone_from(dataset_git_url, repo_path)

                # 2. 创建data目录和下载文件
                data_dir = Path(repo_path / "data")
                data_dir.mkdir(exist_ok=True)

                # 清理旧文件
                for item in data_dir.iterdir():
                    if item.name != ".gitkeep":
                        if item.is_file():
                            item.unlink()
                        elif item.is_dir():
                            shutil.rmtree(item)

                # 3. Git提交
                repo.index.add(["data.dvc", ".gitignore"])
                commit = repo.index.commit(f"Version {version_tag}: {commit_message}")
                commit_hash = commit.hexsha

                # 4. 创建标签
                repo.create_tag(
                    version_tag, message=f"Version {version_tag}: {commit_message}"
                )

                for item in data_dir.iterdir():
                    if item.name != ".gitkeep":
                        if item.is_file():
                            item.unlink()
                        elif item.is_dir():
                            shutil.rmtree(item)

                # 5. 下载文件
                total_size = 0
                for file_ref in file_references:
                    file_content = await self.file_service.download_file(
                        file_ref.file_url
                    )
                    (data_dir / file_ref.filename).write_bytes(file_content)
                    total_size += file_ref.size

                # 6. DVC添加
                dvc_repo = DvcRepo(repo_path)
                dvc_repo.add("data")

                # 7. Git提交
                repo.index.add(["data.dvc", ".gitignore"])
                commit = repo.index.commit(f"Version {version_tag}: {commit_message}")
                commit_hash = commit.hexsha

                # 8. 创建标签
                repo.create_tag(
                    version_tag, message=f"Version {version_tag}: {commit_message}"
                )

                # 9. 推送数据和元数据
                dvc_repo.push()
                repo.remote().push()
                repo.remote().push(tags=True)

                return {
                    "version_id": version_id,
                    "commit_hash": commit_hash,
                    "file_count": len(file_references),
                    "total_size": total_size,
                    "status": "completed",
                }

        except Exception as e:
            task_logger.error(
                "Version creation task failed",
                action="create_version_task",
                version_tag=version_tag,
                error=str(e),
                exc_info=True,
            )
            raise Exception(f"Failed to create version {version_tag}: {e}")

    async def get_dataset_versions_from_git(self, git_repo_url: str) -> List[str]:
        """从Git仓库获取版本标签列表"""
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                # 克隆裸仓库以提高效率
                repo = GitRepo.clone_from(git_repo_url, temp_dir, bare=True)
            tags = sorted(
                [tag.name for tag in repo.tags],
                key=lambda t: repo.tags[t].commit.committed_datetime,
                reverse=True,
            )
            return tags
        except Exception as e:
            logger.error(
                "Failed to get versions from Git repository",
                action="get_versions",
                git_repo_url=git_repo_url,
                error=str(e),
                exc_info=True,
            )
            raise Exception(f"Failed to get versions from {git_repo_url}: {e}")

    async def get_version_file_list(
        self, git_repo_url: str, version_tag: str
    ) -> VersionFileList:
        """获取特定版本的文件列表"""
        try:
            # 使用DVCFileSystem直接查询，无需完整克隆
            fs = DVCFileSystem(git_repo_url, rev=version_tag)
            files_info = fs.find("/", detail=True)

            files = []
            total_size = 0
            for info in files_info:
                if info["type"] == "file":
                    file_data = {
                        "path": info["name"],
                        "size": info["size"],
                        "md5": info.get("md5"),
                    }
                    files.append(file_data)
                    total_size += info["size"]

            return VersionFileList(
                files=files, total_count=len(files), total_size=total_size
            )

        except Exception as e:
            logger.error(
                "Failed to get file list for version",
                action="get_file_list",
                git_repo_url=git_repo_url,
                version_tag=version_tag,
                error=str(e),
                exc_info=True,
            )
            return VersionFileList(files=[], total_count=0, total_size=0)

    async def cleanup_dataset_resources(
        self, task_id: str, dataset_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """清理数据集资源"""
        try:
            git_repo_url = dataset_info.get("git_repo_url")
            dataset_name = dataset_info.get("name")
            # 模拟删除过程
            await asyncio.sleep(1)

            return {"dataset_name": dataset_name, "status": "deleted"}

        except Exception as e:
            logger.error(
                "Failed to cleanup dataset resources",
                action="cleanup_dataset",
                task_id=task_id,
                error=str(e),
                exc_info=True,
            )
            raise Exception(f"Failed to cleanup dataset: {e}")

    async def browse_directory(
        self, git_repo_url: str, version_tag: str, directory_path: str = "/data"
    ) -> DirectoryContent:
        """
        浏览指定目录的内容

        Args:
            git_repo_url: Git仓库URL
            version_tag: 版本标签
            directory_path: 目录路径，默认为根目录

        Returns:
            目录内容信息
        """
        try:
            logger.info(
                "Browsing directory content",
                action="browse_directory",
                git_repo_url=git_repo_url,
                version_tag=version_tag,
                directory_path=directory_path,
            )

            # 标准化路径
            directory_path = directory_path.strip("/")
            if directory_path and not directory_path.endswith("/"):
                directory_path += "/"

            # 使用DVCFileSystem访问文件系统
            fs = DVCFileSystem(git_repo_url, rev=version_tag)

            # 获取目录信息
            try:
                if directory_path:
                    # 非根目录
                    files_info = fs.find(directory_path, detail=True, maxdepth=1)
                else:
                    # 根目录
                    files_info = fs.find("/", detail=True, maxdepth=1)
            except FileNotFoundError:
                logger.warning(
                    "Directory not found",
                    action="browse_directory",
                    directory_path=directory_path,
                )
                return DirectoryContent(
                    current_path=directory_path,
                    parent_path=self._get_parent_path(directory_path),
                    items=[],
                    total_files=0,
                    total_directories=0,
                    total_size=0,
                )

            items = []
            total_files = 0
            total_directories = 0
            total_size = 0

            for info in files_info:
                # 跳过当前目录本身
                if info["name"] == directory_path.rstrip("/"):
                    continue

                item_name = Path(info["name"]).name
                item_path = info["name"]
                item_type = info["type"]

                # 创建FileItem
                file_item = FileItem(
                    name=item_name,
                    path=item_path,
                    type=item_type,
                    is_dvc_tracked=self._is_dvc_tracked(item_path),
                )

                if item_type == "file":
                    file_item.size = info.get("size", 0)
                    file_item.md5 = info.get("md5")
                    # 转换修改时间
                    if "mtime" in info:
                        file_item.modified_time = datetime.fromtimestamp(info["mtime"])

                    total_files += 1
                    total_size += file_item.size or 0
                else:
                    total_directories += 1

                items.append(file_item)

            # 按类型和名称排序（目录在前，文件在后）
            items.sort(key=lambda x: (x.type == "file", x.name.lower()))

            logger.info(
                "Directory browse completed",
                action="browse_directory",
                directory_path=directory_path,
                total_items=len(items),
                total_files=total_files,
                total_directories=total_directories,
            )

            return DirectoryContent(
                current_path=directory_path or "/",
                parent_path=self._get_parent_path(directory_path),
                items=items,
                total_files=total_files,
                total_directories=total_directories,
                total_size=total_size,
            )

        except Exception as e:
            logger.error(
                "Failed to browse directory",
                action="browse_directory",
                git_repo_url=git_repo_url,
                version_tag=version_tag,
                directory_path=directory_path,
                error=str(e),
                exc_info=True,
            )
            raise Exception(f"Failed to browse directory {directory_path}: {e}")

    async def get_file_tree(self, git_repo_url: str, version_tag: str) -> FileTree:
        """
        获取完整文件树结构

        Args:
            git_repo_url: Git仓库URL
            version_tag: 版本标签

        Returns:
            完整文件树
        """
        try:
            logger.info(
                "Building file tree",
                action="get_file_tree",
                git_repo_url=git_repo_url,
                version_tag=version_tag,
            )

            # 使用DVCFileSystem获取所有文件信息
            fs = DVCFileSystem(git_repo_url, rev=version_tag)
            files_info = fs.find("/", detail=True)

            # 构建文件树
            root_node = FileTreeNode(
                name="/",
                path="/",
                type="directory",
                children=[],
                is_dvc_tracked=False,
            )

            total_files = 0
            total_size = 0

            # 创建路径到节点的映射
            path_to_node = {"/": root_node}

            for info in files_info:
                path = info["name"]
                if path == "/":
                    continue

                # 分解路径
                parts = [p for p in path.split("/") if p]
                current_path = ""
                parent_node = root_node

                # 逐级创建目录节点
                for i, part in enumerate(parts):
                    current_path = "/".join([""] + parts[: i + 1])

                    if current_path not in path_to_node:
                        # 创建新节点
                        is_file = i == len(parts) - 1 and info["type"] == "file"

                        node = FileTreeNode(
                            name=part,
                            path=current_path,
                            type="file" if is_file else "directory",
                            children=[] if not is_file else None,
                            is_dvc_tracked=self._is_dvc_tracked(current_path),
                        )

                        if is_file:
                            node.size = info.get("size", 0)
                            total_files += 1
                            total_size += node.size or 0

                        path_to_node[current_path] = node
                        parent_node.children.append(node)

                    parent_node = path_to_node[current_path]

            # 对所有目录的子节点进行排序
            self._sort_tree_nodes(root_node)

            logger.info(
                "File tree built successfully",
                action="get_file_tree",
                total_files=total_files,
                total_nodes=len(path_to_node),
            )

            return FileTree(
                dataset_id=0,  # 这个在API层会被设置
                version_tag=version_tag,
                root=root_node,
                total_files=total_files,
                total_size=total_size,
            )

        except Exception as e:
            logger.error(
                "Failed to build file tree",
                action="get_file_tree",
                git_repo_url=git_repo_url,
                version_tag=version_tag,
                error=str(e),
                exc_info=True,
            )
            raise Exception(f"Failed to build file tree: {e}")

    def _get_parent_path(self, path: str) -> str:
        """获取父目录路径"""
        if not path or path == "/" or path == "":
            return None

        path = path.rstrip("/")
        parent = str(Path(path).parent)
        return parent if parent != "." else "/"

    def _is_dvc_tracked(self, path: str) -> bool:
        """判断文件或目录是否被DVC跟踪"""
        # 简单判断：如果路径以data/开头，则认为是DVC跟踪的
        # 实际项目中可以通过解析.dvc文件来更准确地判断
        return path.startswith("/data/") or path == "/data"

    def _sort_tree_nodes(self, node: FileTreeNode):
        """递归排序树节点（目录在前，文件在后，按名称排序）"""
        if node.children:
            node.children.sort(key=lambda x: (x.type == "file", x.name.lower()))
            for child in node.children:
                self._sort_tree_nodes(child)
