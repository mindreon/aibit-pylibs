import os
from datetime import datetime

from git import Repo

from .logging import get_logger

logger = get_logger(__name__)


class GitRepoUtils:
    """
    一个用于操作 Git 仓库的工具类
    """

    def __init__(self, repo_dir: str):
        """
        初始化 GitUtils
        :param repo_dir: 本地 git 仓库的路径
        """
        self.repo_dir = repo_dir
        if not os.path.exists(self.repo_dir):
            os.makedirs(self.repo_dir)

        # 检查是否已经是git仓库
        if os.path.exists(os.path.join(self.repo_dir, ".git")):
            self.repo = Repo(self.repo_dir)
        else:
            self.repo = Repo.init(self.repo_dir, initial_branch="main")

    def add_files_and_commit(self, files, message: str):
        """
        添加指定文件并提交到本地 Git 仓库

        :param files: 需要添加的文件列表（可以是字符串或字符串列表）
        :param message: 提交信息
        """
        # 检查 files 参数类型
        if isinstance(files, str):
            files = [files]  # 如果是单个字符串，转为列表

        try:
            # 添加文件到暂存区
            self.repo.index.add(files)
            # 提交更改
            self.repo.index.commit(message)
            # 返回最新的 commit id
            return self.get_latest_commit_id()
        except Exception as e:
            # 如果出错，抛出业务异常
            raise Exception(f"添加文件并提交失败: {e}")

    def add_dvc_and_commit(self, message: str):
        """
        添加所有更改并提交
        :param message: 提交信息
        """
        return self.add_files_and_commit(
            ["data.dvc", ".dvc/config", ".dvcignore"], message
        )

    def add_all_and_commit(self, message: str):
        """
        添加所有更改并提交
        :param message: 提交信息
        """
        try:
            self.repo.git.add(A=True)
            self.repo.index.commit(message)
            return self.get_latest_commit_id()
        except Exception as e:
            raise Exception(f"提交失败: {e}")

    def get_latest_commit_id(self) -> str:
        """
        获取最新的 commit id
        """
        return self.repo.head.commit.hexsha

    def add_remote(self, remote_name: str, remote_url: str):
        """
        添加远程仓库
        :param remote_name: 远程仓库名称（通常是 origin）
        :param remote_url: 远程仓库URL
        """
        try:
            # 检查远程仓库是否已存在
            if remote_name in [remote.name for remote in self.repo.remotes]:
                # 更新远程仓库URL
                remote = self.repo.remote(remote_name)
                remote.set_url(remote_url)
                logger.info(
                    "Updated remote repository URL",
                    action="update_remote",
                    remote_name=remote_name,
                    url=remote_url,
                )
            else:
                # 添加新的远程仓库
                self.repo.create_remote(remote_name, remote_url)
                logger.info(
                    "Added remote repository",
                    action="add_remote",
                    remote_name=remote_name,
                    url=remote_url,
                )
        except Exception as e:
            raise Exception(f"添加远程仓库失败: {e}")

    def push(self, remote_name: str = "origin", branch_name: str = "main"):
        """
        推送到远程仓库
        :param remote_name: 远程仓库名称
        :param branch_name: 分支名称
        """
        try:
            remote = self.repo.remote(remote_name)
            remote.push(branch_name)
            logger.info(
                "Successfully pushed to remote",
                action="push",
                remote_name=remote_name,
                branch_name=branch_name,
            )
        except Exception as e:
            raise Exception(f"推送失败: {e}")

    def pull(self, remote_name: str = "origin", branch_name: str = "main"):
        """
        从远程仓库拉取
        :param remote_name: 远程仓库名称
        :param branch_name: 分支名称
        """
        try:
            remote = self.repo.remote(remote_name)
            remote.pull(branch_name)
            logger.info(
                "Successfully pulled from remote",
                action="pull",
                remote_name=remote_name,
                branch_name=branch_name,
            )
        except Exception as e:
            raise Exception(f"拉取失败: {e}")

    def create_tag(self, tag_name: str, message: str = None):
        """
        创建标签
        :param tag_name: 标签名称
        :param message: 标签信息
        """
        try:
            if message:
                tag = self.repo.create_tag(tag_name, message=message)
            else:
                tag = self.repo.create_tag(tag_name)
            return tag
        except Exception as e:
            raise Exception(f"创建标签失败: {e}")

    def push_tag(self, tag_name: str, remote_name: str = "origin"):
        """
        推送标签到远程仓库
        :param tag_name: 标签名称
        :param remote_name: 远程仓库名称
        """
        try:
            remote = self.repo.remote(remote_name)
            remote.push(tag_name)
            logger.info(
                "Pushed tag to remote",
                action="push_tag",
                tag_name=tag_name,
                remote_name=remote_name,
            )
        except Exception as e:
            raise Exception(f"推送标签失败: {e}")

    def list_tags(self):
        """
        列出所有标签（按时间排序）
        """
        try:
            # 按提交时间排序，而不是名称
            tags = sorted(
                self.repo.tags,
                key=lambda tag: tag.commit.committed_datetime,
                reverse=True,
            )
            return [tag.name for tag in tags]
        except Exception as e:
            raise Exception(f"获取标签列表失败: {e}")

    def checkout_tag(self, tag_name: str):
        """
        切换到指定标签
        :param tag_name: 标签名称
        """
        try:
            self.repo.git.checkout(tag_name)
            logger.info("Checked out to tag", action="checkout_tag", tag_name=tag_name)
        except Exception as e:
            raise Exception(f"切换标签失败: {e}")

    def get_current_branch(self):
        """
        获取当前分支名称
        """
        try:
            return self.repo.active_branch.name
        except Exception as e:
            raise Exception(f"获取当前分支失败: {e}")

    def create_branch(self, branch_name: str):
        """
        创建新分支
        :param branch_name: 分支名称
        """
        try:
            new_branch = self.repo.create_head(branch_name)
            new_branch.checkout()
            logger.info(
                "Created and checked out to branch",
                action="create_branch",
                branch_name=branch_name,
            )
            return new_branch
        except Exception as e:
            raise Exception(f"创建分支失败: {e}")

    def get_commit_history(self, max_count: int = 10):
        """
        获取提交历史
        :param max_count: 最大提交数量
        """
        try:
            commits = []
            for commit in self.repo.iter_commits(max_count=max_count):
                commits.append(
                    {
                        "hash": commit.hexsha,
                        "message": commit.message.strip(),
                        "author": str(commit.author),
                        "date": commit.committed_datetime.isoformat(),
                    }
                )
            return commits
        except Exception as e:
            raise Exception(f"获取提交历史失败: {e}")

    def get_file_status(self):
        """
        获取文件状态（已修改、已添加、未跟踪等）
        """
        try:
            status = {
                "modified": [item.a_path for item in self.repo.index.diff(None)],
                "staged": [item.a_path for item in self.repo.index.diff("HEAD")],
                "untracked": self.repo.untracked_files,
            }
            return status
        except Exception as e:
            raise Exception(f"获取文件状态失败: {e}")

    @staticmethod
    def clone_repo(remote_url: str, local_path: str):
        """
        克隆远程仓库
        :param remote_url: 远程仓库URL
        :param local_path: 本地路径
        """
        try:
            cloned_repo = Repo.clone_from(remote_url, local_path)
            logger.info(
                "Successfully cloned repository",
                action="clone_repo",
                remote_url=remote_url,
                local_path=local_path,
            )
            return cloned_repo
        except Exception as e:
            raise Exception(f"克隆仓库失败: {e}")

    def is_repo_clean(self):
        """
        检查仓库是否干净（没有未提交的更改）
        """
        try:
            return not self.repo.is_dirty(untracked_files=True)
        except Exception as e:
            raise Exception(f"检查仓库状态失败: {e}")
