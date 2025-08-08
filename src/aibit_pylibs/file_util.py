import shutil
import tarfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import py7zr
import rarfile
from pydantic import BaseModel

from .logging import get_logger

logger = get_logger(__name__)


# File Browser Schemas
class FileItem(BaseModel):
    name: str
    path: str
    type: str  # "file" or "directory"
    size: Optional[int] = None  # 文件大小，目录为None
    modified_time: Optional[datetime] = None
    md5: Optional[str] = None  # 文件MD5，目录为None
    is_dvc_tracked: bool = False  # 是否被DVC跟踪


class DirectoryContent(BaseModel):
    current_path: str
    parent_path: Optional[str] = None
    items: List[FileItem] = []
    total_files: int = 0
    total_directories: int = 0
    total_size: int = 0


class FileTreeNode(BaseModel):
    """文件树节点"""

    name: str
    path: str
    type: str  # "file" or "directory"
    size: Optional[int] = None
    children: Optional[List["FileTreeNode"]] = None
    is_dvc_tracked: bool = False


class FileTree(BaseModel):
    """完整文件树响应"""

    dataset_id: int
    version_tag: str
    root: FileTreeNode
    total_files: int = 0
    total_size: int = 0


class VersionFileList(BaseModel):
    files: List[Dict[str, Any]]
    total_count: int
    total_size: int


class FileUtils:
    def __init__(self):
        pass

    @staticmethod
    def extract_file(file_path: str, extract_to: Path) -> bool:
        """
        解压文件到指定目录，支持多种格式
        :param file_path: 要解压的文件路径
        :param extract_to: 解压到的目录
        :return: 解压是否成功
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise Exception(f"文件不存在: {file_path}")

        # 确保目标目录存在
        extract_to.mkdir(parents=True, exist_ok=True)

        try:
            # 根据文件扩展名选择解压方式
            suffix = file_path.suffix.lower()

            if suffix in [".zip"]:
                with zipfile.ZipFile(file_path, "r") as zip_ref:
                    zip_ref.extractall(extract_to)
                    logger.info(
                        "File extraction completed",
                        action="extract_file",
                        file_type="zip",
                        source_path=str(file_path),
                        target_path=str(extract_to),
                    )

            elif suffix in [
                ".tar",
                ".tar.gz",
                ".tgz",
                ".tar.bz2",
                ".tbz2",
                ".tar.xz",
                ".txz",
            ]:
                mode = "r"
                if suffix in [".tar.gz", ".tgz"]:
                    mode = "r:gz"
                elif suffix in [".tar.bz2", ".tbz2"]:
                    mode = "r:bz2"
                elif suffix in [".tar.xz", ".txz"]:
                    mode = "r:xz"

                with tarfile.open(file_path, mode) as tar_ref:
                    tar_ref.extractall(extract_to)
                    logger.info(
                        "File extraction completed",
                        action="extract_file",
                        file_type="tar",
                        mode=mode,
                        source_path=str(file_path),
                        target_path=str(extract_to),
                    )

            elif suffix in [".rar"]:
                with rarfile.RarFile(file_path, "r") as rar_ref:
                    rar_ref.extractall(extract_to)
                    logger.info(
                        "File extraction completed",
                        action="extract_file",
                        file_type="rar",
                        source_path=str(file_path),
                        target_path=str(extract_to),
                    )

            elif suffix in [".7z"]:
                with py7zr.SevenZipFile(file_path, mode="r") as seven_zip_ref:
                    seven_zip_ref.extractall(extract_to)
                    logger.info(
                        "File extraction completed",
                        action="extract_file",
                        file_type="7z",
                        source_path=str(file_path),
                        target_path=str(extract_to),
                    )

            else:
                # 如果不是压缩文件，直接复制到目标目录
                shutil.copy2(file_path, extract_to / file_path.name)
                logger.info(
                    "File copied",
                    action="copy_file",
                    source_path=str(file_path),
                    target_path=str(extract_to),
                    filename=file_path.name,
                )

            return True

        except Exception as e:
            logger.error(
                "File extraction failed",
                action="extract_file",
                source_path=str(file_path),
                error=str(e),
                exc_info=True,
            )
            raise Exception(f"解压文件失败 {file_path}: {e}")

    @staticmethod
    def calculate_directory_size(directory: Path) -> int:
        """
        计算目录总大小
        :param directory: 目录路径
        :return: 目录大小（字节）
        """
        total_size = 0
        for file_path in directory.rglob("*"):
            if file_path.is_file():
                total_size += file_path.stat().st_size
        return total_size

    @staticmethod
    def count_files_in_directory(directory: Path) -> int:
        """
        计算目录中文件数量
        :param directory: 目录路径
        :return: 文件数量
        """
        return len([f for f in directory.rglob("*") if f.is_file()])
