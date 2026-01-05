"""
数据索引管理
"""

import os
import json
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any


class IndexManager:
    """管理 data/index.json 的数据索引"""
    
    def __init__(self, index_path: str = None):
        """
        初始化索引管理器
        
        Args:
            index_path: 索引文件路径
        """
        if index_path is None:
            # 默认使用项目根目录下的 data/index.json
            base_dir = Path(__file__).parent.parent.parent
            index_path = base_dir / "data" / "index.json"
        
        self.index_path = Path(index_path)
        self._ensure_index_exists()
    
    def _ensure_index_exists(self):
        """确保索引文件存在"""
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.index_path.exists():
            self._save({"datasets": []})
    
    def _load(self) -> dict:
        """加载索引文件"""
        try:
            with open(self.index_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return {"datasets": []}
    
    def _save(self, data: dict):
        """保存索引文件"""
        with open(self.index_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def get_all(self) -> List[dict]:
        """获取所有数据集"""
        return self._load().get("datasets", [])
    
    def get_by_id(self, dataset_id: str) -> Optional[dict]:
        """根据 ID 获取数据集"""
        for ds in self.get_all():
            if ds.get("id") == dataset_id:
                return ds
        return None
    
    def search(self, keywords: List[str]) -> List[dict]:
        """
        关键词搜索数据集
        
        Args:
            keywords: 关键词列表
            
        Returns:
            匹配的数据集列表（按相关性排序）
        """
        # 停用词
        stop_words = {'的', '是', '在', '有', '和', '与', 'the', 'a', 'an', 'is', 'are', 'of', 'to', 'for'}
        
        # 过滤停用词
        keywords = [k.lower() for k in keywords if k.lower() not in stop_words]
        
        if not keywords:
            return []
        
        results = []
        
        for ds in self.get_all():
            score = 0
            
            # 名称匹配 +10
            name = ds.get("name", "").lower()
            for kw in keywords:
                if kw in name:
                    score += 10
            
            # 描述匹配 +1
            description = ds.get("description", "").lower()
            for kw in keywords:
                if kw in description:
                    score += 1
            
            # 标签匹配 +3
            tags = [t.lower() for t in ds.get("tags", [])]
            for kw in keywords:
                if any(kw in tag for tag in tags):
                    score += 3
            
            # 属性匹配 +1
            properties = ds.get("properties", {})
            props_str = json.dumps(properties).lower()
            for kw in keywords:
                if kw in props_str:
                    score += 1
            
            if score > 0:
                results.append((score, ds))
        
        # 按分数排序，返回前 10 条
        results.sort(key=lambda x: x[0], reverse=True)
        return [ds for _, ds in results[:10]]
    
    def add(self, dataset: dict) -> str:
        """
        添加数据集
        
        Args:
            dataset: 数据集信息
            
        Returns:
            数据集 ID
        """
        data = self._load()
        
        # 确保必要字段
        if "id" not in dataset:
            # 生成 ID：source/name
            source = dataset.get("source_url", "unknown")
            name = dataset.get("name", "dataset")
            from urllib.parse import urlparse
            domain = urlparse(source).netloc.replace(".", "_")
            dataset["id"] = f"{domain}/{name}".lower().replace(" ", "_")
        
        if "crawl_time" not in dataset:
            dataset["crawl_time"] = datetime.now().isoformat()
        
        # 检查是否已存在
        existing_idx = None
        for i, ds in enumerate(data["datasets"]):
            if ds.get("id") == dataset["id"]:
                existing_idx = i
                break
        
        if existing_idx is not None:
            data["datasets"][existing_idx] = dataset
        else:
            data["datasets"].append(dataset)
        
        self._save(data)
        return dataset["id"]
    
    def update_path(self, old_path: str, new_path: str) -> bool:
        """
        更新数据集路径
        
        Args:
            old_path: 旧路径
            new_path: 新路径
            
        Returns:
            是否成功更新
        """
        data = self._load()
        updated = False
        
        for ds in data["datasets"]:
            if ds.get("local_path") == old_path:
                ds["local_path"] = new_path
                updated = True
        
        if updated:
            self._save(data)
        
        return updated
    
    def delete(self, dataset_id: str) -> bool:
        """
        删除数据集记录
        
        Args:
            dataset_id: 数据集 ID
            
        Returns:
            是否成功删除
        """
        data = self._load()
        original_len = len(data["datasets"])
        data["datasets"] = [ds for ds in data["datasets"] if ds.get("id") != dataset_id]
        
        if len(data["datasets"]) < original_len:
            self._save(data)
            return True
        
        return False
    
    def find_by_path(self, path: str) -> Optional[dict]:
        """根据本地路径查找数据集"""
        path = str(Path(path).resolve())
        for ds in self.get_all():
            if str(Path(ds.get("local_path", "")).resolve()) == path:
                return ds
        return None
    
    def find_by_name(self, name: str) -> List[dict]:
        """根据名称模糊查找数据集"""
        name_lower = name.lower()
        results = []
        for ds in self.get_all():
            if name_lower in ds.get("name", "").lower():
                results.append(ds)
        return results
