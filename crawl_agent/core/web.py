"""
网络请求客户端
"""

import os
import time
import requests
from pathlib import Path
from typing import Optional, Callable
from urllib.parse import urljoin, urlparse


class WebClient:
    """HTTP 请求和文件下载客户端"""
    
    def __init__(self, delay: float = 1.0, timeout: int = 30, download_timeout: int = 300):
        """
        初始化 Web 客户端
        
        Args:
            delay: 请求间隔（秒）
            timeout: 请求超时（秒）
            download_timeout: 下载超时（秒）
        """
        self.delay = delay
        self.timeout = timeout
        self.download_timeout = download_timeout
        self.max_retries = 3
        self.last_request_time = 0
        
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })
    
    def _wait_for_delay(self):
        """等待请求间隔"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_request_time = time.time()
    
    def fetch(self, url: str) -> Optional[str]:
        """
        获取网页内容
        
        Args:
            url: 网页 URL
            
        Returns:
            HTML 内容，失败返回 None
        """
        self._wait_for_delay()
        
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                
                # 尝试检测编码
                if response.encoding is None or response.encoding == 'ISO-8859-1':
                    response.encoding = response.apparent_encoding or 'utf-8'
                
                return response.text
                
            except requests.RequestException as e:
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    time.sleep(wait_time)
                else:
                    print(f"获取 {url} 失败: {e}")
                    return None
    
    def download(
        self, 
        url: str, 
        save_path: str, 
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> bool:
        """
        下载文件
        
        Args:
            url: 文件 URL
            save_path: 保存路径
            progress_callback: 进度回调函数 (downloaded, total)
            
        Returns:
            是否成功
        """
        self._wait_for_delay()
        
        for attempt in range(self.max_retries):
            try:
                # 确保目录存在
                Path(save_path).parent.mkdir(parents=True, exist_ok=True)
                
                response = self.session.get(
                    url, 
                    stream=True, 
                    timeout=self.download_timeout
                )
                response.raise_for_status()
                
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                
                with open(save_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if progress_callback:
                                progress_callback(downloaded, total_size)
                
                return True
                
            except requests.RequestException as e:
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    time.sleep(wait_time)
                else:
                    print(f"下载 {url} 失败: {e}")
                    return False
    
    def get_file_size(self, url: str) -> Optional[int]:
        """
        获取文件大小（不下载）
        
        Args:
            url: 文件 URL
            
        Returns:
            文件大小（字节），失败返回 None
        """
        try:
            response = self.session.head(url, timeout=self.timeout)
            return int(response.headers.get('content-length', 0))
        except:
            return None
    
    @staticmethod
    def normalize_url(url: str) -> str:
        """
        标准化 URL（去末尾斜杠、去锚点、去查询参数）
        
        Args:
            url: 原始 URL
            
        Returns:
            标准化后的 URL
        """
        parsed = urlparse(url)
        # 重建 URL，只保留 scheme, netloc, path
        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        # 去末尾斜杠
        return normalized.rstrip('/')
    
    @staticmethod
    def is_same_domain(url1: str, url2: str) -> bool:
        """检查两个 URL 是否同域"""
        return urlparse(url1).netloc == urlparse(url2).netloc
    
    @staticmethod
    def get_domain(url: str) -> str:
        """获取 URL 的域名"""
        return urlparse(url).netloc
