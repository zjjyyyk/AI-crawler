"""
HTML 清洗工具
"""

import re
from typing import List, Tuple, Optional
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup


class HTMLCleaner:
    """HTML 清洗工具"""
    
    # 要移除的标签
    REMOVE_TAGS = [
        'script', 'style', 'nav', 'header', 'footer', 
        'aside', 'form', 'iframe', 'noscript', 'svg',
        'button', 'input', 'select', 'textarea'
    ]
    
    # 下载文件扩展名
    DOWNLOAD_EXTENSIONS = [
        '.gz', '.zip', '.tar', '.bz2', '.xz', '.7z',
        '.csv', '.tsv', '.txt', '.json', '.xml',
        '.parquet', '.feather', '.hdf5', '.h5',
        '.mat', '.npz', '.npy', '.pkl', '.pickle'
    ]
    
    def __init__(self, max_text_length: int = 8000, max_links: int = 200):
        """
        初始化清洗器
        
        Args:
            max_text_length: 最大文本长度
            max_links: 最大链接数
        """
        self.max_text_length = max_text_length
        self.max_links = max_links
    
    def clean(self, html: str, base_url: str, same_domain_only: bool = True) -> Tuple[str, List[dict]]:
        """
        清洗 HTML
        
        Args:
            html: 原始 HTML
            base_url: 基础 URL（用于转换相对路径）
            same_domain_only: 是否只保留同域链接
            
        Returns:
            (清洗后的文本, 链接列表)
        """
        soup = BeautifulSoup(html, 'html.parser')
        
        # 移除噪音标签
        for tag in self.REMOVE_TAGS:
            for element in soup.find_all(tag):
                element.decompose()
        
        # 移除 HTML 注释
        for comment in soup.find_all(string=lambda text: isinstance(text, str) and text.strip().startswith('<!--')):
            comment.extract()
        
        # 提取链接
        links = self._extract_links(soup, base_url, same_domain_only)
        
        # 提取文本
        text = self._extract_text(soup)
        
        # 截断文本
        if len(text) > self.max_text_length:
            text = self._smart_truncate(soup, text)
        
        return text, links
    
    def _extract_links(self, soup: BeautifulSoup, base_url: str, same_domain_only: bool) -> List[dict]:
        """提取并处理链接"""
        links = []
        seen_urls = set()
        base_domain = urlparse(base_url).netloc
        
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href'].strip()
            
            # 跳过空链接、锚点、javascript
            if not href or href.startswith('#') or href.startswith('javascript:'):
                continue
            
            # 转换为绝对路径
            absolute_url = urljoin(base_url, href)
            
            # URL 标准化
            parsed = urlparse(absolute_url)
            normalized_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip('/')
            
            # 跳过已见过的
            if normalized_url in seen_urls:
                continue
            seen_urls.add(normalized_url)
            
            # 同域检查
            if same_domain_only and parsed.netloc != base_domain:
                # 但保留下载链接
                if not self._is_download_link(normalized_url):
                    continue
            
            # 获取链接文本
            link_text = a_tag.get_text(strip=True)
            
            # 判断链接类型
            link_type = "page"
            if self._is_download_link(normalized_url):
                link_type = "download"
            
            links.append({
                "url": normalized_url,
                "text": link_text[:100] if link_text else "",
                "type": link_type
            })
        
        # 分别收集下载链接和页面链接，确保下载链接不会被截断
        download_links = [l for l in links if l['type'] == 'download']
        page_links = [l for l in links if l['type'] == 'page']
        
        # 优先保留所有下载链接，然后填充页面链接
        result = download_links[:self.max_links]
        remaining = self.max_links - len(result)
        if remaining > 0:
            result.extend(page_links[:remaining])
        
        return result
    
    def _is_download_link(self, url: str) -> bool:
        """判断是否为下载链接"""
        path = urlparse(url).path.lower()
        return any(path.endswith(ext) for ext in self.DOWNLOAD_EXTENSIONS)
    
    def _extract_text(self, soup: BeautifulSoup) -> str:
        """提取文本内容"""
        # 获取文本
        text = soup.get_text(separator='\n')
        
        # 清理空白
        lines = []
        prev_empty = False
        
        for line in text.split('\n'):
            line = line.strip()
            
            if not line:
                if not prev_empty:
                    lines.append('')
                    prev_empty = True
            else:
                lines.append(line)
                prev_empty = False
        
        return '\n'.join(lines).strip()
    
    def _smart_truncate(self, soup: BeautifulSoup, text: str) -> str:
        """
        智能截断文本
        
        优先保留：
        1. 表格内容
        2. 列表内容
        3. 链接上下文
        """
        important_parts = []
        
        # 提取表格内容
        for table in soup.find_all('table'):
            table_text = table.get_text(separator=' | ')
            if table_text.strip():
                important_parts.append(f"[表格内容]\n{table_text[:2000]}")
        
        # 提取列表内容
        for ul in soup.find_all(['ul', 'ol']):
            list_text = ul.get_text(separator='\n')
            if list_text.strip():
                important_parts.append(f"[列表内容]\n{list_text[:1000]}")
        
        # 组合重要内容
        important_content = '\n\n'.join(important_parts)
        
        if important_content:
            remaining_space = self.max_text_length - len(important_content) - 100
            if remaining_space > 500:
                # 添加部分原始文本
                return f"{important_content}\n\n[其他内容]\n{text[:remaining_space]}"
            else:
                return important_content[:self.max_text_length]
        
        # 兜底：直接截断
        return text[:self.max_text_length]
    
    @staticmethod
    def extract_download_urls(html: str, base_url: str) -> List[str]:
        """
        从 HTML 中提取所有下载链接
        
        Args:
            html: HTML 内容
            base_url: 基础 URL
            
        Returns:
            下载链接列表
        """
        soup = BeautifulSoup(html, 'html.parser')
        download_urls = []
        seen = set()
        
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href'].strip()
            if not href:
                continue
            
            absolute_url = urljoin(base_url, href)
            
            # 检查是否为下载链接
            path = urlparse(absolute_url).path.lower()
            extensions = ['.gz', '.zip', '.tar', '.csv', '.txt', '.bz2', '.xz']
            
            if any(path.endswith(ext) for ext in extensions):
                if absolute_url not in seen:
                    seen.add(absolute_url)
                    download_urls.append(absolute_url)
        
        return download_urls
