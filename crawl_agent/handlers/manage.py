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

【重要】数据集默认存储在 data/datasets/ 目录下。

请返回 JSON 格式：
{
  "action": "move | delete | copy | list",
  "source": "源路径",
  "target": "目标路径（仅 move/copy 需要）"
}

动作说明：
- move: 移动文件/目录
- delete: 删除文件/目录
- copy: 复制文件/目录
- list: 列出数据集

路径规则：
- 【重要】根据提供的目录结构，推断用户指的是哪个具体目录
  - 例如用户说"snap"，目录中有"snap.stanford.edu"，则应推断为 data/datasets/snap.stanford.edu
  - 例如用户说"facebook数据"，目录中有"snap.stanford.edu/facebook/"，则应推断为 data/datasets/snap.stanford.edu/facebook
- source 和 target 应该是完整路径（以 data/datasets/ 开头）
- 如果用户想把目录内容移动到子目录（下沉操作），这是允许的

示例：
- "把 snap 数据移动到 backup" -> source: "data/datasets/snap.stanford.edu", target: "data/datasets/backup"
- "删除 facebook 数据集" -> source: "data/datasets/snap.stanford.edu/facebook", action: "delete"
- "列出所有数据集" -> action: "list"
- "把 snap 里的数据移到 snap/social 下" -> source: "data/datasets/snap.stanford.edu", target: "data/datasets/snap.stanford.edu/social"

list 操作不需要 source 和 target"""

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
            # 获取当前数据目录结构，帮助LLM理解上下文
            dir_context = self._get_directory_context()
            
            user_message = f"""当前数据目录结构：
{dir_context}

用户指令: {prompt}"""
            
            result = self.llm.chat_json(
                self.PARSE_INTENT_SYSTEM,
                user_message
            )
            
            if "action" not in result:
                raise ValueError("未能解析出操作类型")
            
            return result
            
        except Exception as e:
            raise ValueError(f"解析意图失败: {e}")
    
    def _get_directory_context(self, max_depth: int = 3) -> str:
        """
        获取 data/datasets 目录结构作为上下文
        
        Args:
            max_depth: 最大遍历深度
            
        Returns:
            目录结构的文本表示
        """
        base_dir = Path(__file__).parent.parent.parent / "data" / "datasets"
        
        if not base_dir.exists():
            return "data/datasets/ (目录不存在)"
        
        lines = ["data/datasets/"]
        
        def _scan_dir(path: Path, prefix: str, depth: int):
            if depth > max_depth:
                return
            
            try:
                items = sorted(path.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower()))
            except PermissionError:
                return
            
            # 限制每层显示的项目数
            dirs = [item for item in items if item.is_dir()]
            files = [item for item in items if item.is_file()]
            
            # 显示所有目录
            for i, item in enumerate(dirs):
                is_last_dir = (i == len(dirs) - 1) and not files
                connector = "└── " if is_last_dir else "├── "
                lines.append(f"{prefix}{connector}{item.name}/")
                
                # 递归子目录
                new_prefix = prefix + ("    " if is_last_dir else "│   ")
                _scan_dir(item, new_prefix, depth + 1)
            
            # 显示文件（最多5个，超出显示数量）
            if files:
                shown_files = files[:5]
                for i, item in enumerate(shown_files):
                    is_last = (i == len(shown_files) - 1) and (len(files) <= 5)
                    connector = "└── " if is_last else "├── "
                    # 显示文件大小
                    try:
                        size = item.stat().st_size
                        size_str = self._format_size(size)
                        lines.append(f"{prefix}{connector}{item.name} ({size_str})")
                    except:
                        lines.append(f"{prefix}{connector}{item.name}")
                
                if len(files) > 5:
                    lines.append(f"{prefix}└── ... 还有 {len(files) - 5} 个文件")
        
        _scan_dir(base_dir, "", 1)
        
        return "\n".join(lines) if len(lines) > 1 else "data/datasets/ (空目录)"
    
    def _format_size(self, size: int) -> str:
        """格式化文件大小"""
        if size < 1024:
            return f"{size}B"
        elif size < 1024 * 1024:
            return f"{size/1024:.1f}KB"
        elif size < 1024 * 1024 * 1024:
            return f"{size/1024/1024:.1f}MB"
        else:
            return f"{size/1024/1024/1024:.1f}GB"
    
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
        
        # 尝试补全 data/datasets 前缀
        base_dir = Path(__file__).parent.parent.parent / "data" / "datasets"
        prefixed_path = base_dir / source.lstrip('/')
        if prefixed_path.exists():
            return prefixed_path.resolve()
        
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
        
        # 解析目标路径（也支持自动补全 data/datasets 前缀）
        target_path = Path(target)
        if not target_path.is_absolute():
            # 相对路径，检查是否需要补全 data/datasets
            base_dir = Path(__file__).parent.parent.parent / "data" / "datasets"
            if target.startswith("data/datasets/") or target.startswith("data\\datasets\\"):
                target_path = Path(__file__).parent.parent.parent / target
            else:
                target_path = base_dir / target.lstrip('/')
        target_path = target_path.resolve()
        
        # 检查是否尝试将目录移动到自身内部（下沉操作）
        is_sink_operation = False
        try:
            # 检查 target_path 是否在 source_path 内部
            target_path.relative_to(source_path)
            is_sink_operation = True
        except ValueError:
            # 不在内部，正常移动
            pass
        
        if is_sink_operation:
            # 这是"下沉"操作：把目录内容移动到其子目录
            return self._handle_sink_move(source_path, target_path)
        
        # 检查是否是"上浮"操作（把子目录内容移动到父目录）
        is_float_operation = False
        try:
            # 检查 source_path 是否在 target_path 内部
            source_path.relative_to(target_path)
            is_float_operation = True
        except ValueError:
            pass
        
        if is_float_operation:
            # 这是"上浮"操作：把子目录内容移动到父目录
            return self._handle_float_move(source_path, target_path)
        
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
            updated_count = self.index.update_path(str(source_path), str(target_path))
            
            self.display.print_success(f"已移动到: {target_path}")
            if updated_count > 0:
                self.display.print_status(f"已更新 {updated_count} 条索引记录")
            
            return {
                "success": True,
                "action": "move",
                "source": str(source_path),
                "target": str(target_path)
            }
            
        except Exception as e:
            self.display.print_error(f"移动失败: {e}")
            return {"success": False, "error": str(e)}
    
    def _handle_sink_move(self, source_path: Path, target_path: Path) -> dict:
        """
        处理"下沉"移动：将目录内容移动到其子目录中
        
        例如：把 snap.stanford.edu/ 下的内容移动到 snap.stanford.edu/social/
        """
        self.display.print_status(f"检测到下沉操作：将目录内容移动到子目录")
        
        # 获取源目录下的所有项目（排除目标子目录）
        items_to_move = []
        target_relative = target_path.relative_to(source_path)
        target_first_part = target_relative.parts[0] if target_relative.parts else None
        
        for item in source_path.iterdir():
            # 跳过目标子目录的第一级目录
            if item.name == target_first_part:
                continue
            items_to_move.append(item)
        
        if not items_to_move:
            self.display.print_warning("源目录下没有需要移动的内容")
            return {"success": False, "error": "没有内容需要移动"}
        
        # 显示预览
        self.display.print_status(f"即将执行下沉移动操作:")
        print(f"  源目录: {source_path}")
        print(f"  目标目录: {target_path}")
        print(f"  将移动以下 {len(items_to_move)} 个项目:")
        for item in items_to_move[:10]:  # 最多显示10个
            item_type = "📁" if item.is_dir() else "📄"
            print(f"    {item_type} {item.name}")
        if len(items_to_move) > 10:
            print(f"    ... 还有 {len(items_to_move) - 10} 个项目")
        
        # 确认
        if not self.display.confirm("确认执行此操作？"):
            self.display.print_warning("操作已取消")
            return {"success": False, "cancelled": True}
        
        try:
            # 确保目标目录存在
            target_path.mkdir(parents=True, exist_ok=True)
            
            moved_count = 0
            updated_index_count = 0
            for item in items_to_move:
                dest = target_path / item.name
                shutil.move(str(item), str(dest))
                moved_count += 1
                
                # 更新索引中的路径
                updated_index_count += self.index.update_path(str(item), str(dest))
            
            self.display.print_success(f"已将 {moved_count} 个项目移动到: {target_path}")
            if updated_index_count > 0:
                self.display.print_status(f"已更新 {updated_index_count} 条索引记录")
            
            return {
                "success": True,
                "action": "sink_move",
                "source": str(source_path),
                "target": str(target_path),
                "moved_count": moved_count
            }
            
        except Exception as e:
            self.display.print_error(f"移动失败: {e}")
            return {"success": False, "error": str(e)}
    
    def _handle_float_move(self, source_path: Path, target_path: Path) -> dict:
        """
        处理"上浮"移动：将子目录内容移动到父目录中
        
        例如：把 snap.stanford.edu/social/ 下的内容移动到 snap.stanford.edu/
        """
        self.display.print_status(f"检测到上浮操作：将子目录内容移动到父目录")
        
        # 获取源目录下的所有项目
        items_to_move = list(source_path.iterdir())
        
        if not items_to_move:
            self.display.print_warning("源目录下没有需要移动的内容")
            return {"success": False, "error": "没有内容需要移动"}
        
        # 检查冲突：目标目录中是否已存在同名项目
        conflicts = []
        for item in items_to_move:
            dest = target_path / item.name
            if dest.exists() and dest != source_path:
                conflicts.append(item.name)
        
        # 显示预览
        self.display.print_status(f"即将执行上浮移动操作:")
        print(f"  源目录: {source_path}")
        print(f"  目标目录: {target_path}")
        print(f"  将移动以下 {len(items_to_move)} 个项目:")
        for item in items_to_move[:10]:
            item_type = "📁" if item.is_dir() else "📄"
            print(f"    {item_type} {item.name}")
        if len(items_to_move) > 10:
            print(f"    ... 还有 {len(items_to_move) - 10} 个项目")
        
        if conflicts:
            self.display.print_warning(f"⚠️ 以下 {len(conflicts)} 个项目在目标目录已存在，将被覆盖:")
            for name in conflicts[:5]:
                print(f"    - {name}")
            if len(conflicts) > 5:
                print(f"    ... 还有 {len(conflicts) - 5} 个")
        
        # 确认
        if not self.display.confirm("确认执行此操作？"):
            self.display.print_warning("操作已取消")
            return {"success": False, "cancelled": True}
        
        try:
            moved_count = 0
            updated_index_count = 0
            
            for item in items_to_move:
                dest = target_path / item.name
                
                # 如果目标已存在且不是源目录本身，需要先删除
                if dest.exists() and dest != source_path:
                    if dest.is_dir():
                        shutil.rmtree(str(dest))
                    else:
                        dest.unlink()
                
                shutil.move(str(item), str(dest))
                moved_count += 1
                
                # 更新索引中的路径
                updated_index_count += self.index.update_path(str(item), str(dest))
            
            # 移动完成后，删除空的源目录
            if source_path.exists() and not any(source_path.iterdir()):
                source_path.rmdir()
                self.display.print_status(f"已删除空目录: {source_path.name}")
            
            self.display.print_success(f"已将 {moved_count} 个项目移动到: {target_path}")
            if updated_index_count > 0:
                self.display.print_status(f"已更新 {updated_index_count} 条索引记录")
            
            return {
                "success": True,
                "action": "float_move",
                "source": str(source_path),
                "target": str(target_path),
                "moved_count": moved_count
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
        
        # 解析目标路径（也支持自动补全 data/datasets 前缀）
        target_path = Path(target)
        if not target_path.is_absolute():
            base_dir = Path(__file__).parent.parent.parent / "data" / "datasets"
            if target.startswith("data/datasets/") or target.startswith("data\\datasets\\"):
                target_path = Path(__file__).parent.parent.parent / target
            else:
                target_path = base_dir / target.lstrip('/')
        target_path = target_path.resolve()
        
        # 检查是否尝试将目录复制到自身内部
        try:
            target_path.relative_to(source_path)
            self.display.print_error(f"无法将目录复制到其自身内部")
            return {"success": False, "error": "无法将目录复制到其自身内部"}
        except ValueError:
            pass
        
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
