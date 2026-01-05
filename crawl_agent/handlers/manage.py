"""
管理处理器
"""

import os
import shutil
from pathlib import Path
from typing import List, Dict, Any, Optional

from ..core.llm import LLMClient
from ..core.index import IndexManager
from ..utils.display import Display


class ManageHandler:
    """数据管理处理器"""
    
    # 解析意图的系统提示
    PARSE_INTENT_SYSTEM = """你是一个文件管理助手。解析用户的文件管理指令。

请返回 JSON 格式：
{
  "action": "move | delete | copy | list",
  "source": "源路径或数据集描述",
  "target": "目标路径（仅 move/copy 需要）"
}

动作说明：
- move: 移动文件/目录
- delete: 删除文件/目录
- copy: 复制文件/目录
- list: 列出数据集

注意：
- source 可以是具体路径或数据集名称/描述
- target 是目标路径
- list 操作不需要 source 和 target"""

    def __init__(self):
        self.llm = LLMClient()
        self.index = IndexManager()
        self.display = Display()
    
    def handle(self, prompt: str) -> dict:
        """
        处理管理请求
        
        Args:
            prompt: 用户的自然语言指令
            
        Returns:
            操作结果
        """
        # 1. 解析意图
        self.display.print_status("正在解析指令...")
        intent = self._parse_intent(prompt)
        
        action = intent.get("action", "").lower()
        source = intent.get("source", "")
        target = intent.get("target", "")
        
        self.display.print_status(f"动作: {action}")
        if source:
            self.display.print_status(f"源: {source}")
        if target:
            self.display.print_status(f"目标: {target}")
        
        # 2. 执行对应操作
        if action == "list":
            return self._handle_list()
        elif action == "move":
            return self._handle_move(source, target)
        elif action == "copy":
            return self._handle_copy(source, target)
        elif action == "delete":
            return self._handle_delete(source)
        else:
            self.display.print_error(f"未知操作: {action}")
            return {"success": False, "error": f"未知操作: {action}"}
    
    def _parse_intent(self, prompt: str) -> dict:
        """解析用户意图"""
        try:
            result = self.llm.chat_json(
                self.PARSE_INTENT_SYSTEM,
                f"用户指令: {prompt}"
            )
            
            if "action" not in result:
                raise ValueError("未能解析出操作类型")
            
            return result
            
        except Exception as e:
            raise ValueError(f"解析意图失败: {e}")
    
    def _resolve_source(self, source: str) -> Optional[Path]:
        """
        解析源路径
        
        Args:
            source: 路径或数据集描述
            
        Returns:
            解析后的路径
        """
        # 先尝试作为路径
        path = Path(source)
        if path.exists():
            return path.resolve()
        
        # 尝试在索引中查找
        datasets = self.index.find_by_name(source)
        
        if not datasets:
            # 尝试关键词搜索
            keywords = source.split()
            datasets = self.index.search(keywords)
        
        if len(datasets) == 1:
            local_path = datasets[0].get("local_path")
            if local_path and Path(local_path).exists():
                return Path(local_path).resolve()
        elif len(datasets) > 1:
            # 让用户选择
            self.display.print_warning(f"找到多个匹配的数据集:")
            for i, ds in enumerate(datasets, 1):
                print(f"  {i}. {ds.get('name')} - {ds.get('local_path')}")
            
            choice = input("请输入序号选择: ").strip()
            try:
                idx = int(choice) - 1
                if 0 <= idx < len(datasets):
                    local_path = datasets[idx].get("local_path")
                    if local_path and Path(local_path).exists():
                        return Path(local_path).resolve()
            except ValueError:
                pass
        
        return None
    
    def _handle_list(self) -> dict:
        """列出所有数据集"""
        datasets = self.index.get_all()
        self.display.print_datasets(datasets)
        
        return {
            "success": True,
            "action": "list",
            "count": len(datasets)
        }
    
    def _handle_move(self, source: str, target: str) -> dict:
        """移动数据"""
        # 解析源路径
        source_path = self._resolve_source(source)
        if not source_path:
            self.display.print_error(f"找不到源路径: {source}")
            return {"success": False, "error": f"找不到: {source}"}
        
        target_path = Path(target).resolve()
        
        # 显示预览
        self.display.print_status(f"即将执行移动操作:")
        print(f"  从: {source_path}")
        print(f"  到: {target_path}")
        
        # 确认
        if not self.display.confirm("确认执行此操作？"):
            self.display.print_warning("操作已取消")
            return {"success": False, "cancelled": True}
        
        try:
            # 确保目标父目录存在
            target_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 移动
            shutil.move(str(source_path), str(target_path))
            
            # 更新索引
            self.index.update_path(str(source_path), str(target_path))
            
            self.display.print_success(f"已移动到: {target_path}")
            
            return {
                "success": True,
                "action": "move",
                "source": str(source_path),
                "target": str(target_path)
            }
            
        except Exception as e:
            self.display.print_error(f"移动失败: {e}")
            return {"success": False, "error": str(e)}
    
    def _handle_copy(self, source: str, target: str) -> dict:
        """复制数据"""
        # 解析源路径
        source_path = self._resolve_source(source)
        if not source_path:
            self.display.print_error(f"找不到源路径: {source}")
            return {"success": False, "error": f"找不到: {source}"}
        
        target_path = Path(target).resolve()
        
        # 显示预览
        self.display.print_status(f"即将执行复制操作:")
        print(f"  从: {source_path}")
        print(f"  到: {target_path}")
        
        # 确认
        if not self.display.confirm("确认执行此操作？"):
            self.display.print_warning("操作已取消")
            return {"success": False, "cancelled": True}
        
        try:
            # 确保目标父目录存在
            target_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 复制
            if source_path.is_dir():
                shutil.copytree(str(source_path), str(target_path))
            else:
                shutil.copy2(str(source_path), str(target_path))
            
            self.display.print_success(f"已复制到: {target_path}")
            
            return {
                "success": True,
                "action": "copy",
                "source": str(source_path),
                "target": str(target_path)
            }
            
        except Exception as e:
            self.display.print_error(f"复制失败: {e}")
            return {"success": False, "error": str(e)}
    
    def _handle_delete(self, source: str) -> dict:
        """删除数据"""
        # 解析源路径
        source_path = self._resolve_source(source)
        if not source_path:
            self.display.print_error(f"找不到源路径: {source}")
            return {"success": False, "error": f"找不到: {source}"}
        
        # 显示预览
        self.display.print_warning(f"即将删除:")
        print(f"  路径: {source_path}")
        
        if source_path.is_dir():
            # 统计目录大小
            total_size = sum(f.stat().st_size for f in source_path.rglob('*') if f.is_file())
            file_count = sum(1 for f in source_path.rglob('*') if f.is_file())
            print(f"  文件数: {file_count}")
            print(f"  总大小: {total_size / 1024 / 1024:.2f} MB")
        
        # 确认（删除操作需要二次确认）
        if not self.display.confirm("⚠️ 此操作不可恢复！确认删除？"):
            self.display.print_warning("操作已取消")
            return {"success": False, "cancelled": True}
        
        try:
            # 从索引中查找并删除记录
            dataset = self.index.find_by_path(str(source_path))
            if dataset:
                self.index.delete(dataset.get("id"))
            
            # 删除文件
            if source_path.is_dir():
                shutil.rmtree(str(source_path))
            else:
                source_path.unlink()
            
            self.display.print_success(f"已删除: {source_path}")
            
            return {
                "success": True,
                "action": "delete",
                "deleted": str(source_path)
            }
            
        except Exception as e:
            self.display.print_error(f"删除失败: {e}")
            return {"success": False, "error": str(e)}
