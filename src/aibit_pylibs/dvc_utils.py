import asyncio
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from dvc.api import DVCFileSystem
from dvc.repo import Repo as DvcRepo
from git import Repo as GitRepo

from .file_utils import FileTree, FileTreeNode, FileUtils
from .git_utils import GitRepoUtils
from .gitea_provider import GiteaProvider
from .logging import get_logger

logger = get_logger(__name__)


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

    async def initialize_dvc_repo(
        self,
        id: str,
        repo_name: str,
        description: str,
        prefix_path: str,
        tenant_name: str,
        file_path: str,
    ) -> Dict[str, Any]:
        """
        初始化Git仓库和DVC配置，解压用户上传的文件并管理
        :param id: 仓库ID
        :param repo_name: 仓库名称
        :param description: 仓库描述
        :param prefix_path: 仓库路径前缀
        :param tenant_name: 租户名称
        :param file_path: 用户上传的压缩文件路径
        """

        try:
            # 创建临时目录进行所有操作
            with tempfile.TemporaryDirectory() as temp_base_dir:
                temp_base = Path(temp_base_dir)

                # 1. 解压用户上传的文件到临时目录
                extract_dir = Path(f"{temp_base}/{id}/data")
                FileUtils.extract_file(file_path, extract_dir)

                # 计算解压后的文件统计信息
                total_files = FileUtils.count_files_in_directory(extract_dir)
                total_size = FileUtils.calculate_directory_size(extract_dir)

                # 2. 设置Git仓库
                repo_dir = Path(f"{temp_base}/{id}")

                # 创建Git仓库
                git_repo = GitRepoUtils(str(repo_dir))

                # 创建或获取组织
                await self.gitea_provider.create_org(tenant_name)

                # 创建或获取仓库
                repo_info = await self.gitea_provider.get_repo(tenant_name, repo_name)

                if repo_info:
                    repo_url = repo_info["clone_url"]
                else:
                    repo_data = await self.gitea_provider.create_repo(
                        tenant_name, repo_name
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
                git_repo.add_remote("origin", repo_url_with_token)

                # 5. 初始化DVC
                dvc_repo = DvcRepo.init(repo_dir, no_scm=True)

                # 6. 配置DVC远程存储
                dvc_remote_url = f"s3://{prefix_path}/{id}"
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

                # 9. 创建README文件
                (repo_dir / "README.md").write_text(description, encoding="utf-8")

                # 10. 提交初始版本到Git
                commit_hash = git_repo.add_dvc_and_commit(
                    f"初始化 {repo_name} - 来源文件: {Path(file_path).name}"
                )

                # 11. 创建初始版本v1标签
                git_repo.create_tag(
                    "v1", f"初始版本 - {total_files}个文件，{total_size:,}字节"
                )

                # 12. 推送到Git+DVC远程仓库
                dvc_repo.push()
                git_repo.push("origin", "main")
                git_repo.push_tag("v1", "origin")

                return {
                    "id": id,
                    "repo_url": repo_url,
                    "commit_hash": commit_hash,
                    "file_count": total_files,
                    "total_size": total_size,
                    "status": "initialized",
                }

        except Exception as e:
            logger.error(
                "Dataset initialization task failed",
                action="initialize_dvc_repo",
                id=id,
                repo_name=repo_name,
                tenant_name=tenant_name,
                file_path=file_path,
                error=str(e),
                exc_info=True,
            )
            raise Exception(f"初始化数据集任务失败: {e}")

    async def create_dvc_version(
        self,
        id: str,
        repo_url: str,
        version_tag: str,
        file_references: List[FileTreeNode],
        commit_message: str,
        file_service=None,  # Add file_service parameter
    ) -> Dict[str, Any]:
        """处理版本创建的异步任务（基于文件引用）"""
        if not file_service:
            raise ValueError("file_service is required for downloading files")

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                repo_path = Path(temp_dir) / "repo"

                # 1. 克隆仓库
                repo = GitRepo.clone_from(repo_url, repo_path)

                # 2. 创建data目录
                data_dir = Path(repo_path / "data")
                data_dir.mkdir(exist_ok=True)

                # 清理旧文件（只执行一次）
                self._cleanup_directory(data_dir)

                # 3. 下载文件
                total_size = 0
                for file_ref in file_references:
                    try:
                        file_content = await file_service.download_file(
                            file_ref.file_url
                        )
                        (data_dir / file_ref.filename).write_bytes(file_content)
                        total_size += file_ref.size
                    except Exception as e:
                        logger.warning(
                            "Failed to download file",
                            action="download_file",
                            filename=file_ref.filename,
                            error=str(e),
                        )
                        continue

                # 4. DVC添加
                dvc_repo = DvcRepo(repo_path)
                dvc_repo.add("data")

                # 5. Git提交和标签创建（只执行一次）
                repo.index.add(["data.dvc", ".gitignore"])
                commit = repo.index.commit(f"Version {version_tag}: {commit_message}")
                commit_hash = commit.hexsha

                # 创建标签
                repo.create_tag(
                    version_tag, message=f"Version {version_tag}: {commit_message}"
                )

                # 6. 推送数据和元数据
                try:
                    dvc_repo.push()
                    repo.remote().push()
                    repo.remote().push(tags=True)
                except Exception as e:
                    logger.error(
                        "Failed to push to remote",
                        action="push_remote",
                        version_tag=version_tag,
                        error=str(e),
                    )
                    raise

                return {
                    "version_id": version_tag,
                    "commit_hash": commit_hash,
                    "file_count": len(file_references),
                    "total_size": total_size,
                    "status": "completed",
                }

        except Exception as e:
            logger.error(
                "Version creation task failed",
                action="create_dvc_version",
                id=id,
                version_tag=version_tag,
                error=str(e),
                exc_info=True,
            )
            raise Exception(f"Failed to create version {version_tag}: {e}")

    async def get_versions(self, repo_url: str) -> List[str]:
        """从Git仓库获取版本标签列表"""
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                # 克隆裸仓库以提高效率
                repo = GitRepo.clone_from(repo_url, temp_dir, bare=True)
                # 在with块内处理tags，避免作用域问题
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
                repo_url=repo_url,
                error=str(e),
                exc_info=True,
            )
            raise Exception(f"Failed to get versions from {repo_url}: {e}")

    async def get_filesystem_info(
        self,
        repo_url: str,
        version_tag: str,
        path: str = "data/",
        mode: str = "list",
        max_depth: int = None,
    ) -> FileTree:
        """
        统一的文件系统信息获取方法

        Args:
            repo_url: Git仓库URL
            version_tag: 版本标签
            path: 查询路径，默认为根目录
            mode: 查询模式 - 'list'(文件列表), 'browse'(目录浏览), 'tree'(完整树)
            max_depth: 最大深度

        Returns:
            根据模式返回不同格式的数据
        """
        try:
            logger.info(
                "Getting filesystem info",
                action="get_filesystem_info",
                repo_url=repo_url,
                version_tag=version_tag,
                path=path,
                mode=mode,
            )

            # 标准化路径
            start_path = path.strip("/")
            if start_path and not start_path.endswith("/"):
                start_path += "/"

            # 使用DVCFileSystem访问文件系统
            fs = DVCFileSystem(repo_url, rev=version_tag)

            # 获取完整文件树结构
            files_info = fs.find(start_path, detail=True, maxdepth=max_depth)

            root_node = FileTreeNode(
                name=start_path,
                path=start_path,
                type="directory",
                children=[],
            )

            total_files = 0
            total_size = 0
            path_to_node = {path: root_node}

            for info in files_info:
                path = info["name"]
                if path == start_path.rstrip("/"):
                    continue

                parts = [p for p in path.split("/") if p]
                current_path = ""
                parent_node = root_node

                for i, part in enumerate(parts):
                    current_path = "/".join([""] + parts[: i + 1])

                    if current_path not in path_to_node:
                        is_file = i == len(parts) - 1 and info["type"] == "file"

                        node = FileTreeNode(
                            name=part,
                            path=current_path,
                            type="file" if is_file else "directory",
                            children=[] if not is_file else None,
                        )

                        if is_file:
                            node.size = info.get("size", 0)
                            node.md5 = info.get("md5")
                            if "mtime" in info:
                                node.modified_time = datetime.fromtimestamp(
                                    info["mtime"]
                                )
                            total_files += 1
                            total_size += node.size or 0

                        path_to_node[current_path] = node
                        parent_node.children.append(node)

                    parent_node = path_to_node[current_path]

            self._sort_tree_nodes(root_node)

            return FileTree(
                entity_id=0,
                version_tag=version_tag,
                root=root_node,
                total_files=total_files,
                total_size=total_size,
            )

        except Exception as e:
            logger.error(
                "Failed to get filesystem info",
                action="get_filesystem_info",
                repo_url=repo_url,
                version_tag=version_tag,
                path=path,
                mode=mode,
                error=str(e),
                exc_info=True,
            )
            raise Exception(f"Failed to get filesystem info for {path}: {e}")

    async def cleanup_resources(self, repo_url: str) -> Dict[str, Any]:
        """清理资源"""
        try:
            # 模拟删除过程
            await asyncio.sleep(1)

            return {"repo_url": repo_url, "status": "deleted"}

        except Exception as e:
            logger.error(
                "Failed to cleanup resources",
                action="cleanup_resources",
                error=str(e),
                exc_info=True,
            )
            raise Exception(f"Failed to cleanup resources: {e}")

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

    def _cleanup_directory(self, data_dir: Path) -> None:
        """清理目录中的文件（保留.gitkeep）"""
        for item in data_dir.iterdir():
            if item.name != ".gitkeep":
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)

    def _sort_tree_nodes(self, node: FileTreeNode):
        """递归排序树节点（目录在前，文件在后，按名称排序）"""
        if node.children:
            node.children.sort(key=lambda x: (x.type == "file", x.name.lower()))
            for child in node.children:
                self._sort_tree_nodes(child)
