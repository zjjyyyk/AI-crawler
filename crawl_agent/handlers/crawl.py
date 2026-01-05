"""
爬取处理器
"""

import os
import json
import time
import threading
from pathlib import Path
from datetime import datetime
from typing import Set, List, Dict, Any, Optional
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..core.llm import LLMClient
from ..core.web import WebClient
from ..core.index import IndexManager
from ..utils.display import Display
from ..utils.html_cleaner import HTMLCleaner


class CrawlHandler:
    """爬取处理器"""
    
    # 最大并发数
    MAX_WORKERS = 128
    
    # 解析意图的系统提示
    PARSE_INTENT_SYSTEM = """你是一个爬虫助手。根据用户的指令，解析出爬取任务的参数。

请返回 JSON 格式：
{
  "url": "要爬取的根URL",
  "save_path": "保存路径，默认为 data/datasets/{域名}",
  "criteria": "筛选条件，描述要爬取什么类型的数据",
  "max_depth": 2
}

注意：
- url 必须是完整的 URL（包含 http:// 或 https://）
- save_path 如果用户未指定，则使用默认值
- criteria 用于筛选要下载的数据
- max_depth 是爬取深度，默认为 2"""

    # 分析页面的系统提示
    ANALYZE_PAGE_SYSTEM = """你是一个数据集分析助手。你的任务是从给定的链接列表中选择符合条件的链接。

请返回 JSON 格式：
{
  "resources": [
    {
      "name": "数据集名称",
      "description": "数据集描述",
      "download_indices": [0, 1, 2],
      "properties": {
        "nodes": 数字或null,
        "edges": 数字或null,
        "directed": true/false/null
      }
    }
  ],
  "follow_indices": [0, 1, 2]
}

【重要规则】：
1. download_indices 是下载链接的索引号数组（从 DOWNLOAD_LINKS 中选择）
2. follow_indices 是页面链接的索引号数组（从 PAGE_LINKS 中选择）
3. 只填写数字索引，不要填写 URL
4. properties 只填页面中明确存在的数字，没有就填 null
5. 如果没有符合条件的链接，返回空数组"""

    def __init__(self):
        self.llm = LLMClient()
        self.web = WebClient()
        self.index = IndexManager()
        self.display = Display()
        self.cleaner = HTMLCleaner()
        
        # 爬取状态（线程安全）
        self.visited_urls: Set[str] = set()
        self.visited_lock = threading.Lock()
        self.downloaded_resources: List[dict] = []
        self.download_lock = threading.Lock()
        self.errors: List[str] = []
        self.errors_lock = threading.Lock()
        
        # 爬取历史文件
        self.history_path = Path(__file__).parent.parent.parent / "data" / "crawl_history.json"
    
    def handle(self, prompt: str) -> dict:
        """
        处理爬取请求
        
        Args:
            prompt: 用户的自然语言指令
            
        Returns:
            爬取结果摘要
        """
        start_time = time.time()
        
        try:
            # 1. 解析用户意图
            self.display.print_status("正在解析指令...")
            intent = self._parse_intent(prompt)
            
            self.display.print_status(f"目标URL: {intent['url']}")
            self.display.print_status(f"保存路径: {intent['save_path']}")
            self.display.print_status(f"筛选条件: {intent['criteria']}")
            self.display.print_status(f"最大深度: {intent['max_depth']}")
            
            # 2. 加载历史记录
            self._load_history()
            
            # 3. 使用广度优先 + 并行爬取
            self.display.print_status("开始爬取...")
            self._crawl_bfs(
                root_url=intent['url'],
                save_path=intent['save_path'],
                criteria=intent['criteria'],
                max_depth=intent['max_depth']
            )
            
            # 4. 保存历史记录
            self._save_history()
            
            # 5. 生成摘要
            duration = time.time() - start_time
            summary = {
                "pages_visited": len(self.visited_urls),
                "resources_downloaded": len(self.downloaded_resources),
                "datasets_added": len(self.downloaded_resources),
                "save_path": intent['save_path'],
                "duration": duration,
                "errors": self.errors
            }
            
            self.display.print_crawl_summary(summary)
            
            return summary
            
        except KeyboardInterrupt:
            self.display.print_warning("用户中断，正在保存当前状态...")
            self._save_history()
            raise
        except Exception as e:
            self.display.print_error(f"爬取失败: {e}")
            raise
    
    def _parse_intent(self, prompt: str) -> dict:
        """解析用户意图"""
        try:
            result = self.llm.chat_json(
                self.PARSE_INTENT_SYSTEM,
                f"用户指令: {prompt}"
            )
            
            # 验证必要字段
            if "url" not in result or not result["url"]:
                raise ValueError("未能解析出目标 URL")
            
            # 设置默认值
            if not result.get("save_path"):
                domain = urlparse(result["url"]).netloc.replace(".", "_")
                result["save_path"] = f"data/datasets/{domain}"
            
            result.setdefault("criteria", "所有数据集")
            result.setdefault("max_depth", 2)
            
            return result
            
        except Exception as e:
            raise ValueError(f"解析意图失败: {e}")
    
    def _crawl_bfs(self, root_url: str, save_path: str, criteria: str, max_depth: int):
        """广度优先 + 并行爬取"""
        # 当前层的 URL 列表
        current_level = [root_url]
        
        for depth in range(max_depth + 1):
            if not current_level:
                break
            
            self.display.print_status(f"--- 深度 {depth}: {len(current_level)} 个页面 ---")
            
            # 过滤已访问的 URL
            urls_to_visit = []
            for url in current_level:
                normalized = self.web.normalize_url(url)
                with self.visited_lock:
                    if normalized not in self.visited_urls:
                        self.visited_urls.add(normalized)
                        urls_to_visit.append(url)
            
            if not urls_to_visit:
                self.display.print_status(f"深度 {depth}: 所有页面已访问过，跳过")
                break
            
            # 收集下一层的 URL
            next_level = []
            next_level_lock = threading.Lock()
            
            # 并行处理当前层
            with ThreadPoolExecutor(max_workers=min(self.MAX_WORKERS, len(urls_to_visit))) as executor:
                futures = {
                    executor.submit(
                        self._process_page, url, save_path, criteria, depth
                    ): url for url in urls_to_visit
                }
                
                for future in as_completed(futures):
                    url = futures[future]
                    try:
                        follow_links = future.result()
                        if follow_links:
                            with next_level_lock:
                                next_level.extend(follow_links)
                    except Exception as e:
                        self.display.print_error(f"处理失败 {url}: {e}")
            
            current_level = next_level
    
    def _process_page(self, url: str, save_path: str, criteria: str, depth: int) -> List[str]:
        """
        处理单个页面
        
        Returns:
            需要跟进的链接列表
        """
        normalized_url = self.web.normalize_url(url)
        self.display.print_status(f"[深度 {depth}] 访问: {normalized_url}")
        
        # 获取页面内容
        html = self.web.fetch(url)
        if not html:
            with self.errors_lock:
                self.errors.append(f"无法获取: {url}")
            return []
        
        # 清洗 HTML
        clean_text, links = self.cleaner.clean(html, url)
        
        # 截断过长的内容
        if len(clean_text) > 8000:
            clean_text = clean_text[:8000] + "\n...[内容已截断]"
        
        # 分析页面
        try:
            analysis = self._analyze_page(clean_text, links, criteria)
        except Exception as e:
            self.display.print_warning(f"分析页面失败: {e}")
            with self.errors_lock:
                self.errors.append(f"分析失败: {url}")
            return []
        
        # 下载资源
        for resource in analysis.get("resources", []):
            self._download_resource(resource, save_path, url)
        
        return analysis.get("follow_links", [])
    
    def _analyze_page(self, text: str, links: List[dict], criteria: str) -> dict:
        """分析页面内容"""
        # 分离下载链接和页面链接
        download_links = []
        page_links = []
        
        for link in links[:100]:  # 增加到100个链接
            if link['type'] == 'download':
                download_links.append(link)
            else:
                page_links.append(link)
        
        # 调试：显示找到的链接统计
        self.display.print_status(f"发现 {len(download_links)} 个下载链接, {len(page_links)} 个页面链接")
        
        # 构建带索引的链接列表
        download_list = "\n".join([
            f"  [{i}] {link['url']} (文本: {link['text'][:50]})"
            for i, link in enumerate(download_links)
        ]) or "  (无)"
        
        page_list = "\n".join([
            f"  [{i}] {link['url']} (文本: {link['text'][:50]})"
            for i, link in enumerate(page_links)
        ]) or "  (无)"
        
        user_prompt = f"""页面内容:
{text}

=== DOWNLOAD_LINKS (下载链接，通过索引号选择) ===
{download_list}

=== PAGE_LINKS (页面链接，通过索引号选择) ===
{page_list}

筛选条件: {criteria}

请返回要选择的链接索引号。download_indices 从 DOWNLOAD_LINKS 选，follow_indices 从 PAGE_LINKS 选。"""

        try:
            result = self.llm.chat_json(self.ANALYZE_PAGE_SYSTEM, user_prompt)
        except Exception as e:
            self.display.print_warning(f"LLM 调用失败: {e}")
            return {"resources": [], "follow_links": []}
        
        # 根据索引提取实际的 URL
        validated_resources = []
        for res in result.get("resources", []):
            valid_urls = []
            indices = res.get("download_indices", [])
            
            for idx in indices:
                if isinstance(idx, int) and 0 <= idx < len(download_links):
                    valid_urls.append(download_links[idx]['url'])
                else:
                    self.display.print_warning(f"无效的下载索引: {idx}")
            
            if valid_urls:
                validated_resources.append({
                    "name": res.get("name", "unknown"),
                    "description": res.get("description", ""),
                    "download_urls": valid_urls,
                    "properties": res.get("properties", {})
                })
        
        # 根据索引提取跟进链接
        validated_follow_links = []
        for idx in result.get("follow_indices", []):
            if isinstance(idx, int) and 0 <= idx < len(page_links):
                validated_follow_links.append(page_links[idx]['url'])
            else:
                self.display.print_warning(f"无效的页面索引: {idx}")
        
        self.display.print_status(f"选择了 {len(validated_resources)} 个资源, {len(validated_follow_links)} 个跟进链接")
        
        return {
            "resources": validated_resources,
            "follow_links": validated_follow_links
        }
    
    def _download_resource(self, resource: dict, base_save_path: str, source_url: str):
        """下载资源并更新索引"""
        name = resource.get("name", "unknown")
        download_urls = resource.get("download_urls", [])
        
        if not download_urls:
            return
        
        # 创建数据集目录
        safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in name)
        dataset_path = Path(base_save_path) / safe_name
        dataset_path.mkdir(parents=True, exist_ok=True)
        
        downloaded_files = []
        
        for url in download_urls:
            try:
                # 获取文件名
                filename = Path(urlparse(url).path).name
                if not filename:
                    filename = "data"
                
                save_file = dataset_path / filename
                
                self.display.print_status(f"下载: {filename}")
                
                # 直接下载，使用简单的进度显示
                success = self._download_with_progress(url, str(save_file))
                
                if success and save_file.exists():
                    file_size = save_file.stat().st_size
                    downloaded_files.append({
                        "name": filename,
                        "size": file_size
                    })
                    self.display.print_success(f"已下载: {filename} ({file_size / 1024 / 1024:.2f} MB)")
                else:
                    with self.errors_lock:
                        self.errors.append(f"下载失败: {url}")
                    
            except Exception as e:
                with self.errors_lock:
                    self.errors.append(f"下载错误: {url} - {e}")
        
        if downloaded_files:
            # 保存 meta.json
            meta = {
                "name": name,
                "description": resource.get("description", ""),
                "source_url": source_url,
                "download_urls": download_urls,
                "properties": resource.get("properties", {}),
                "files": downloaded_files,
                "crawl_time": datetime.now().isoformat()
            }
            
            meta_path = dataset_path / "meta.json"
            with open(meta_path, 'w', encoding='utf-8') as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)
            
            # 更新索引
            dataset_info = {
                "name": name,
                "source_url": source_url,
                "local_path": str(dataset_path),
                "description": resource.get("description", ""),
                "properties": resource.get("properties", {}),
                "tags": [],
                "files": downloaded_files
            }
            
            with self.download_lock:
                self.index.add(dataset_info)
                self.downloaded_resources.append(dataset_info)
    
    def _download_with_progress(self, url: str, save_path: str) -> bool:
        """带进度显示的下载"""
        import requests
        
        try:
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            # 确保目录存在
            Path(save_path).parent.mkdir(parents=True, exist_ok=True)
            
            downloaded = 0
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # 简单的进度输出（每 1MB 输出一次）
                        if total_size > 0 and downloaded % (1024 * 1024) < 8192:
                            percent = downloaded / total_size * 100
                            print(f"\r  进度: {percent:.1f}% ({downloaded / 1024 / 1024:.1f} MB)", end="", flush=True)
            
            print()  # 换行
            return True
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self.display.print_warning(f"文件不存在 (404): {url}")
            else:
                self.display.print_warning(f"HTTP 错误 {e.response.status_code}: {url}")
            return False
        except Exception as e:
            self.display.print_warning(f"下载失败: {e}")
            return False
    
    def _load_history(self):
        """加载爬取历史"""
        try:
            if self.history_path.exists():
                with open(self.history_path, 'r', encoding='utf-8') as f:
                    history = json.load(f)
                    self.visited_urls = set(history.get("visited_urls", []))
                    self.display.print_status(f"已加载 {len(self.visited_urls)} 条历史记录")
        except Exception:
            self.visited_urls = set()
    
    def _save_history(self):
        """保存爬取历史"""
        try:
            self.history_path.parent.mkdir(parents=True, exist_ok=True)
            
            history = {
                "visited_urls": list(self.visited_urls),
                "last_updated": datetime.now().isoformat()
            }
            
            with open(self.history_path, 'w', encoding='utf-8') as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.display.print_warning(f"保存历史记录失败: {e}")
