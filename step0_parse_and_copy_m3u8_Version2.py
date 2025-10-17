#!/usr/bin/env python3
"""
Step 0: 解析 M3U8 文件并复制音乐文件到 Apple Music 媒体文件夹

功能：
1. 解析 M3U8 文件，提取所有音乐文件路径
2. 读取音频文件的元数据（艺人、专辑）
3. 按照 艺人/专辑/歌曲名 的目录结构复制到 ~/Music/Music/Media.localized/Music/
4. 生成 imported_tracks.json 供后续使用
5. 输出详细的复制状态和日志
"""

import os
import sys
import json
import shutil
import hashlib
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from urllib.parse import unquote, urlparse

try:
    from mutagen import File as MutagenFile
    from mutagen.mp4 import MP4
    from mutagen.id3 import ID3
    from mutagen.flac import FLAC
except ImportError:
    print("错误: 需要安装 mutagen 库来读取音频元数据")
    print("请运行: pip3 install mutagen")
    sys.exit(1)


class M3U8Importer:
    """M3U8 文件解析和音乐文件导入器"""
    
    def __init__(self, m3u8_path: str, target_base_dir: str = None):
        """
        初始化导入器
        
        Args:
            m3u8_path: M3U8 文件路径
            target_base_dir: 目标基础文件夹，默认为 Apple Music 媒体文件夹
        """
        self.m3u8_path = Path(m3u8_path).expanduser().resolve()
        
        if target_base_dir:
            self.target_base_dir = Path(target_base_dir).expanduser().resolve()
        else:
            self.target_base_dir = Path.home() / "Music" / "Music" / "Media.localized" / "Music"
        
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = Path(f"step0_{self.timestamp}.log")
        self.output_json = Path("imported_tracks.json")
        
        # 统计信息
        self.stats = {
            "total_entries": 0,
            "valid_files": 0,
            "missing_files": 0,
            "metadata_read_success": 0,
            "metadata_read_failed": 0,
            "copied_success": 0,
            "copied_skipped": 0,  # 已存在的文件
            "copied_failed": 0,
            "total_size": 0,
            "start_time": datetime.now().isoformat(),
            "end_time": None
        }
        
        self.results: List[Dict] = []
    
    def log(self, message: str, level: str = "INFO"):
        """写入日志"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] [{level}] {message}"
        print(log_message)
        
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_message + "\n")
    
    def sanitize_filename(self, name: str) -> str:
        """
        清理文件/文件夹名称，移除非法字符
        
        Args:
            name: 原始名称
            
        Returns:
            清理后的名称
        """
        if not name:
            return "Unknown"
        
        # 移除或替换非法字符
        # macOS 文件系统不允许: / :
        # 其他需要注意的字符: \ ? * < > | "
        illegal_chars = {
            '/': '∕',  # 使用 Unicode 除号替代
            ':': '∶',  # 使用 Unicode 比号替代
            '\\': '⧵',
            '?': '？',  # 全角问号
            '*': '＊',  # 全角星号
            '<': '＜',
            '>': '＞',
            '|': '｜',
            '"': '"',
        }
        
        for illegal, replacement in illegal_chars.items():
            name = name.replace(illegal, replacement)
        
        # 移除前后空格和点号（macOS 隐藏文件）
        name = name.strip(' .')
        
        # 如果清理后为空，使用默认值
        if not name:
            name = "Unknown"
        
        return name
    
    def extract_metadata(self, file_path: Path) -> Dict[str, Optional[str]]:
        """
        提取音频文件的元数据
        
        Args:
            file_path: 音频文件路径
            
        Returns:
            包含艺人、专辑、标题等信息的字典
        """
        metadata = {
            "artist": None,
            "album_artist": None,
            "album": None,
            "title": None,
            "track_number": None,
            "year": None,
            "genre": None
        }
        
        try:
            audio = MutagenFile(file_path, easy=False)
            
            if audio is None:
                self.log(f"无法识别音频格式: {file_path.name}", "WARNING")
                return metadata
            
            # MP4/M4A 格式 (Apple Music 常用格式)
            if isinstance(audio, MP4):
                metadata["artist"] = audio.get("\xa9ART", [None])[0]
                metadata["album_artist"] = audio.get("aART", [None])[0]
                metadata["album"] = audio.get("\xa9alb", [None])[0]
                metadata["title"] = audio.get("\xa9nam", [None])[0]
                metadata["year"] = audio.get("\xa9day", [None])[0]
                metadata["genre"] = audio.get("\xa9gen", [None])[0]
                
                # Track number
                track_info = audio.get("trkn", [None])[0]
                if track_info:
                    metadata["track_number"] = track_info[0]
            
            # MP3 格式 (ID3 tags)
            elif hasattr(audio, 'tags') and audio.tags:
                tags = audio.tags
                
                # 尝试不同的标签格式
                metadata["artist"] = (
                    tags.get("TPE1", tags.get("TPE2", None))
                )
                metadata["album_artist"] = tags.get("TPE2", None)
                metadata["album"] = tags.get("TALB", None)
                metadata["title"] = tags.get("TIT2", None)
                metadata["year"] = tags.get("TDRC", tags.get("TYER", None))
                metadata["genre"] = tags.get("TCON", None)
                metadata["track_number"] = tags.get("TRCK", None)
                
                # 将标签对象转换为字符串
                for key in metadata:
                    if metadata[key] and hasattr(metadata[key], 'text'):
                        metadata[key] = str(metadata[key].text[0])
            
            # FLAC 格式
            elif isinstance(audio, FLAC):
                metadata["artist"] = audio.get("artist", [None])[0]
                metadata["album_artist"] = audio.get("albumartist", [None])[0]
                metadata["album"] = audio.get("album", [None])[0]
                metadata["title"] = audio.get("title", [None])[0]
                metadata["year"] = audio.get("date", [None])[0]
                metadata["genre"] = audio.get("genre", [None])[0]
                metadata["track_number"] = audio.get("tracknumber", [None])[0]
            
            # 清理空字符串
            for key in metadata:
                if metadata[key] == "":
                    metadata[key] = None
            
            self.stats["metadata_read_success"] += 1
            
        except Exception as e:
            self.log(f"读取元数据失败: {file_path.name}, 错误: {e}", "WARNING")
            self.stats["metadata_read_failed"] += 1
        
        return metadata
    
    def generate_target_path(self, source_path: Path, metadata: Dict) -> Path:
        """
        根据元数据生成目标文件路径（艺人/专辑/歌曲名）
        
        Args:
            source_path: 源文件路径
            metadata: 元数据字典
            
        Returns:
            目标文件路径
        """
        # 优先使用 Album Artist，其次使用 Artist
        artist = metadata.get("album_artist") or metadata.get("artist") or "Unknown Artist"
        album = metadata.get("album") or "Unknown Album"
        
        # 清理文件夹名称
        artist_clean = self.sanitize_filename(artist)
        album_clean = self.sanitize_filename(album)
        
        # 构建目标目录: 艺人/专辑
        target_dir = self.target_base_dir / artist_clean / album_clean
        target_dir.mkdir(parents=True, exist_ok=True)
        
        # 保持原始文件名（或使用标题）
        if metadata.get("title"):
            # 如果有 Track Number，可以添加到文件名前
            if metadata.get("track_number"):
                try:
                    track_num = int(str(metadata["track_number"]).split('/')[0])
                    filename = f"{track_num:02d} - {metadata['title']}{source_path.suffix}"
                except:
                    filename = f"{metadata['title']}{source_path.suffix}"
            else:
                filename = f"{metadata['title']}{source_path.suffix}"
            
            filename = self.sanitize_filename(filename)
        else:
            filename = source_path.name
        
        target_path = target_dir / filename
        
        # 处理文件名冲突
        if target_path.exists() and not self._files_identical(source_path, target_path):
            counter = 1
            stem = target_path.stem
            suffix = target_path.suffix
            
            while target_path.exists():
                new_filename = f"{stem} ({counter}){suffix}"
                target_path = target_dir / new_filename
                counter += 1
                
                if counter > 100:
                    raise Exception(f"文件名冲突次数过多: {filename}")
        
        return target_path
    
    def parse_m3u8(self) -> List[str]:
        """
        解析 M3U8 文件，提取所有音乐文件路径
        
        Returns:
            音乐文件路径列表
        """
        self.log(f"开始解析 M3U8 文件: {self.m3u8_path}")
        
        if not self.m3u8_path.exists():
            self.log(f"M3U8 文件不存在: {self.m3u8_path}", "ERROR")
            return []
        
        file_paths = []
        m3u8_dir = self.m3u8_path.parent
        
        try:
            with open(self.m3u8_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    
                    # 跳过空行和注释行
                    if not line or line.startswith('#'):
                        continue
                    
                    self.stats["total_entries"] += 1
                    
                    # 处理相对路径和绝对路径
                    file_path = self._resolve_path(line, m3u8_dir)
                    
                    if file_path:
                        file_paths.append(str(file_path))
                        self.stats["valid_files"] += 1
                    else:
                        self.log(f"第 {line_num} 行: 无法解析路径: {line}", "WARNING")
            
            self.log(f"解析完成: 共 {self.stats['total_entries']} 行，找到 {self.stats['valid_files']} 个有效文件")
            
        except Exception as e:
            self.log(f"解析 M3U8 文件失败: {e}", "ERROR")
            return []
        
        return file_paths
    
    def _resolve_path(self, path_str: str, base_dir: Path) -> Optional[Path]:
        """
        解析文件路径（支持相对路径、绝对路径、URL 格式）
        
        Args:
            path_str: 路径字符串
            base_dir: M3U8 文件所在目录
            
        Returns:
            解析后的 Path 对象，如果文件不存在则返回 None
        """
        try:
            # 处理 URL 格式
            if path_str.startswith('file://'):
                parsed = urlparse(path_str)
                path_str = unquote(parsed.path)
            
            # 处理 URL 编码
            path_str = unquote(path_str)
            
            # 转换为 Path 对象
            file_path = Path(path_str)
            
            # 如果是相对路径，相对于 M3U8 文件所在目录
            if not file_path.is_absolute():
                file_path = (base_dir / file_path).resolve()
            else:
                file_path = file_path.expanduser().resolve()
            
            # 检查文件是否存在
            if file_path.exists() and file_path.is_file():
                return file_path
            else:
                self.log(f"文件不存在: {file_path}", "WARNING")
                self.stats["missing_files"] += 1
                return None
                
        except Exception as e:
            self.log(f"解析路径失败: {path_str}, 错误: {e}", "ERROR")
            return None
    
    def _files_identical(self, file1: Path, file2: Path) -> bool:
        """
        检查两个文件是否内容相同（通过 MD5 哈希）
        
        Args:
            file1: 文件1路径
            file2: 文件2路径
            
        Returns:
            是否相同
        """
        try:
            # 先比较文件大小
            if file1.stat().st_size != file2.stat().st_size:
                return False
            
            # 计算 MD5 哈希
            hash1 = self._calculate_md5(file1)
            hash2 = self._calculate_md5(file2)
            
            return hash1 == hash2
            
        except Exception as e:
            self.log(f"比较文件失败: {e}", "WARNING")
            return False
    
    def _calculate_md5(self, file_path: Path) -> str:
        """
        计算文件的 MD5 哈希
        
        Args:
            file_path: 文件路径
            
        Returns:
            MD5 哈希字符串
        """
        md5_hash = hashlib.md5()
        
        with open(file_path, 'rb') as f:
            # 分块读取，避免大文件占用过多内存
            for chunk in iter(lambda: f.read(8192), b""):
                md5_hash.update(chunk)
        
        return md5_hash.hexdigest()
    
    def copy_files(self, file_paths: List[str]):
        """
        复制音乐文件到目标文件夹（按艺人/专辑结构）
        
        Args:
            file_paths: 音乐文件路径列表
        """
        self.log(f"开始复制文件到: {self.target_base_dir}")
        self.log(f"目录结构: 艺人/专辑/歌曲名")
        
        # 确保基础目标文件夹存在
        self.target_base_dir.mkdir(parents=True, exist_ok=True)
        
        total_files = len(file_paths)
        
        for idx, source_path_str in enumerate(file_paths, 1):
            source_path = Path(source_path_str)
            
            try:
                # 读取元数据
                metadata = self.extract_metadata(source_path)
                
                # 生成目标路径
                target_path = self.generate_target_path(source_path, metadata)
                
                # 检查是否需要复制
                if target_path.exists() and self._files_identical(source_path, target_path):
                    artist = metadata.get("album_artist") or metadata.get("artist") or "Unknown"
                    album = metadata.get("album") or "Unknown"
                    self.log(f"[{idx}/{total_files}] 跳过（已存在）: {artist}/{album}/{target_path.name}")
                    self.stats["copied_skipped"] += 1
                    status = "skipped"
                else:
                    # 复制文件
                    shutil.copy2(source_path, target_path)
                    file_size = target_path.stat().st_size
                    self.stats["total_size"] += file_size
                    self.stats["copied_success"] += 1
                    status = "success"
                    
                    # 显示进度
                    artist = metadata.get("album_artist") or metadata.get("artist") or "Unknown"
                    album = metadata.get("album") or "Unknown"
                    size_mb = file_size / (1024 * 1024)
                    self.log(f"[{idx}/{total_files}] 复制成功: {artist}/{album}/{target_path.name} ({size_mb:.2f} MB)")
                
                # 记录结果
                self.results.append({
                    "source_path": str(source_path),
                    "target_path": str(target_path),
                    "filename": target_path.name,
                    "size_bytes": target_path.stat().st_size,
                    "status": status,
                    "md5": self._calculate_md5(target_path),
                    "metadata": metadata
                })
                
            except Exception as e:
                self.log(f"[{idx}/{total_files}] 复制失败: {source_path.name}, 错误: {e}", "ERROR")
                self.stats["copied_failed"] += 1
                
                # 记录失败结果
                self.results.append({
                    "source_path": str(source_path),
                    "target_path": None,
                    "filename": source_path.name,
                    "size_bytes": 0,
                    "status": "failed",
                    "error": str(e),
                    "metadata": {}
                })
    
    def save_results(self):
        """保存结果到 JSON 文件"""
        self.stats["end_time"] = datetime.now().isoformat()
        
        output_data = {
            "metadata": {
                "step": "step0_parse_and_copy",
                "m3u8_file": str(self.m3u8_path),
                "target_base_directory": str(self.target_base_dir),
                "directory_structure": "Artist/Album/Track",
                "timestamp": self.timestamp,
                "log_file": str(self.log_file)
            },
            "statistics": self.stats,
            "tracks": self.results
        }
        
        try:
            with open(self.output_json, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            
            self.log(f"结果已保存到: {self.output_json}")
            
        except Exception as e:
            self.log(f"保存结果失败: {e}", "ERROR")
    
    def print_summary(self):
        """打印统计摘要"""
        print("\n" + "=" * 70)
        print("Step 0 执行摘要")
        print("=" * 70)
        print(f"M3U8 文件:           {self.m3u8_path}")
        print(f"目标基础文件夹:      {self.target_base_dir}")
        print(f"目录结构:            艺人/专辑/歌曲名")
        print("-" * 70)
        print(f"总条目数:            {self.stats['total_entries']}")
        print(f"有效文件数:          {self.stats['valid_files']}")
        print(f"缺失文件数:          {self.stats['missing_files']}")
        print(f"元数据读取成功:      {self.stats['metadata_read_success']}")
        print(f"元数据读取失败:      {self.stats['metadata_read_failed']}")
        print("-" * 70)
        print(f"复制成功:            {self.stats['copied_success']}")
        print(f"跳过（已存在）:      {self.stats['copied_skipped']}")
        print(f"复制失败:            {self.stats['copied_failed']}")
        print(f"总大小:              {self.stats['total_size'] / (1024**3):.2f} GB")
        print("-" * 70)
        print(f"日志文件:            {self.log_file}")
        print(f"输出文件:            {self.output_json}")
        print("=" * 70)
        
        if self.stats['copied_failed'] > 0:
            print("\n⚠️  部分文件复制失败，请检查日志文件")
        elif self.stats['missing_files'] > 0:
            print("\n⚠️  部分文件在源路径中找不到")
        elif self.stats['metadata_read_failed'] > 0:
            print("\n⚠️  部分文件的元数据读取失败（使用默认值）")
        else:
            print("\n✅ 所有文件处理完成！")
    
    def run(self):
        """执行完整的导入流程"""
        try:
            # 解析 M3U8
            file_paths = self.parse_m3u8()
            
            if not file_paths:
                self.log("没有找到有效的音乐文件，退出", "ERROR")
                return False
            
            # 复制文件
            self.copy_files(file_paths)
            
            # 保存结果
            self.save_results()
            
            # 打印摘要
            self.print_summary()
            
            return True
            
        except Exception as e:
            self.log(f"执行失败: {e}", "ERROR")
            import traceback
            self.log(traceback.format_exc(), "ERROR")
            return False


def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("用法: python3 step0_parse_and_copy_m3u8.py <m3u8_file_path> [target_base_directory]")
        print()
        print("参数:")
        print("  m3u8_file_path         M3U8 文件路径（必需）")
        print("  target_base_directory  目标基础文件夹（可选，默认为 ~/Music/Music/Media.localized/Music）")
        print()
        print("说明:")
        print("  脚本会读取音频文件的元数据（艺人、专辑），")
        print("  并按照 '艺人/专辑/歌曲名' 的目录结构复制文件。")
        print()
        print("依赖:")
        print("  pip3 install mutagen")
        print()
        print("示例:")
        print("  python3 step0_parse_and_copy_m3u8.py ~/Downloads/playlist.m3u8")
        print("  python3 step0_parse_and_copy_m3u8.py ~/Downloads/playlist.m3u8 ~/Music/Custom")
        sys.exit(1)
    
    m3u8_path = sys.argv[1]
    target_base_dir = sys.argv[2] if len(sys.argv) > 2 else None
    
    # 创建导入器并执行
    importer = M3U8Importer(m3u8_path, target_base_dir)
    success = importer.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()