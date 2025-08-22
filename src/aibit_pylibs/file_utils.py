import os
import shutil
import tarfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import py7zr
import rarfile
from pydantic import BaseModel

from .logging import get_logger

logger = get_logger(__name__)


class FileTreeNode(BaseModel):
    """文件树节点"""

    name: str
    path: str
    type: str  # "file" or "directory"
    size: Optional[int] = None
    modified_time: Optional[datetime] = None
    md5: Optional[str] = None  # 文件MD5，目录为None
    children: Optional[List["FileTreeNode"]] = None


class FileTree(BaseModel):
    """完整文件树响应"""

    entity_id: int
    version_tag: str
    root: FileTreeNode
    total_files: int = 0
    total_size: int = 0


class FileUtils:
    def __init__(self):
        pass

    @staticmethod
    def _validate_archive_safety(file_path: Path, extract_to: Path) -> None:
        """
        验证压缩文件的安全性，防止路径遍历和zip bomb攻击
        :param file_path: 压缩文件路径
        :param extract_to: 解压目标目录
        """
        # 验证压缩文件内容安全性
        suffix = file_path.suffix.lower()

        if suffix == ".zip":
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                # 检查zip bomb
                # 检查路径遍历攻击
                for zinfo in zip_ref.filelist:
                    if os.path.isabs(zinfo.filename) or ".." in zinfo.filename:
                        raise Exception(f"检测到路径遍历攻击: {zinfo.filename}")

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
                # 检查路径遍历攻击
                for member in tar_ref.getmembers():
                    if os.path.isabs(member.name) or ".." in member.name:
                        raise Exception(f"检测到路径遍历攻击: {member.name}")

    @staticmethod
    def uncompress_file(file_path: str, extract_to: Path) -> bool:
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

        # 验证安全性
        FileUtils._validate_archive_safety(file_path, extract_to)

        try:
            # 根据文件扩展名选择解压方式
            suffix = file_path.suffix.lower()

            if suffix in [".zip"]:
                with zipfile.ZipFile(file_path, "r") as zip_ref:
                    # 安全解压，防止路径遍历
                    for member in zip_ref.filelist:
                        full_path = extract_to / member.filename
                        if not str(full_path).startswith(str(extract_to)):
                            raise Exception(f"路径遍历攻击检测: {member.filename}")

                        # 如果是目录，创建目录
                        if member.is_dir():
                            full_path.mkdir(parents=True, exist_ok=True)
                        else:
                            # 确保父目录存在
                            full_path.parent.mkdir(parents=True, exist_ok=True)
                            # 提取文件
                            with (
                                zip_ref.open(member) as source,
                                open(full_path, "wb") as target,
                            ):
                                target.write(source.read())

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
                    for member in tar_ref.getmembers():
                        full_path = extract_to / member.name
                        # 确保路径在目标目录内
                        if not str(full_path).startswith(str(extract_to)):
                            raise Exception(f"路径遍历攻击检测: {member.name}")

                        tar_ref.extract(member, extract_to)

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
                    for info in rar_ref.infolist():
                        full_path = extract_to / info.filename
                        # 确保路径在目标目录内
                        if not str(full_path).startswith(str(extract_to)):
                            raise Exception(f"路径遍历攻击检测: {info.filename}")

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
                    for info in seven_zip_ref.list():
                        full_path = extract_to / info.filename
                        # 确保路径在目标目录内
                        if not str(full_path).startswith(str(extract_to)):
                            raise Exception(f"路径遍历攻击检测: {info.filename}")

                    seven_zip_ref.extractall(extract_to)
                    logger.info(
                        "File extraction completed",
                        action="extract_file",
                        file_type="7z",
                        source_path=str(file_path),
                        target_path=str(extract_to),
                    )

            else:
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
    def _compress_zip(source_path: str, output_path: str):
        """使用 zip 格式进行压缩"""
        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            if os.path.isfile(source_path):
                # 处理单个文件
                arcname = os.path.basename(source_path)
                zipf.write(source_path, arcname=arcname)
            elif os.path.isdir(source_path):
                # 处理文件夹
                for root, dirs, files in os.walk(source_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, start=source_path)
                        zipf.write(file_path, arcname=arcname)

    @staticmethod
    def _compress_tar(source_path: str, output_path: str, mode: str = "w:gz"):
        """使用 tar 系列格式进行压缩 (如 .tar.gz, .tar.bz2)"""
        with tarfile.open(output_path, mode) as tarf:
            if os.path.isfile(source_path):
                arcname = os.path.basename(source_path)
                tarf.add(source_path, arcname=arcname)
            elif os.path.isdir(source_path):
                for root, dirs, files in os.walk(source_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, start=source_path)
                        tarf.add(file_path, arcname=arcname)

    @staticmethod
    def compress_file(source_path: str, output_path: str):
        """
        根据输出文件的后缀名，自动选择压缩方式。

        :param source_path: 要压缩的源文件或文件夹的路径。
        :param output_path: 输出的压缩文件的路径。
        """
        if not os.path.exists(source_path):
            logger.error(
                f"source path not found",
                action="compress_file",
                source_path=source_path,
                output_path=output_path,
            )
            return

        logger.info(
            f"begin to compress",
            action="compress_file",
            source_path=source_path,
            output_path=output_path,
        )

        suffix = output_path.split(".")[-1].lower()

        try:
            if suffix == "zip":
                FileUtils._compress_zip(source_path, output_path)
            elif suffix == "tar":
                FileUtils._compress_tar(source_path, output_path, mode="w:")
            elif suffix == "tar.gz":
                FileUtils._compress_tar(source_path, output_path, mode="w:gz")
            elif suffix == "tar.bz2":
                FileUtils._compress_tar(source_path, output_path, mode="w:bz2")
            elif suffix == "tar.xz":
                FileUtils._compress_tar(source_path, output_path, mode="w:xz")
            else:
                logger.warning(f"unsupported suffix", suffix=suffix)
                return

            logger.info(
                f"compress success",
                action="compress_file",
                source_path=source_path,
                output_path=output_path,
            )
        except Exception as e:
            logger.error(
                f"compress failed",
                action="compress_file",
                source_path=source_path,
                output_path=output_path,
            )

    @staticmethod
    def calculate_directory_size(directory: Path) -> int:
        """
        计算目录总大小
        :param directory: 目录路径
        :return: 目录大小（字节）
        """
        total_size = 0
        try:
            for file_path in directory.rglob("*"):
                if file_path.is_file():
                    total_size += file_path.stat().st_size

            logger.debug(
                "Directory size calculated",
                path=str(directory),
                total_bytes=total_size,
            )

            return total_size

        except Exception as e:
            logger.error(
                "Failed to calculate directory size",
                path=str(directory),
                error=str(e),
                exc_info=True,
            )
            raise Exception(f"Failed to calculate directory size: {e}")

    @staticmethod
    def count_files_in_directory(directory: Path) -> int:
        """
        计算目录中文件数量
        :param directory: 目录路径
        :return: 文件数量
        """
        try:
            file_count = len([f for f in directory.rglob("*") if f.is_file()])

            logger.debug(
                "File count calculated",
                path=str(directory),
                file_count=file_count,
            )

            return file_count

        except Exception as e:
            logger.error(
                "Failed to count files in directory",
                path=str(directory),
                error=str(e),
                exc_info=True,
            )
            raise Exception(f"Failed to count files: {e}")
