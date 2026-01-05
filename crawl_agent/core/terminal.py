"""
命令执行器（带权限控制）
"""

import os
import subprocess
from pathlib import Path
from typing import Tuple, Optional, Set


class TerminalExecutor:
    """Shell 命令执行器，带白名单权限控制"""
    
    # 默认允许的命令
    DEFAULT_ALLOWED = {
        "python", "python3", "pip", "pip3",
        "ls", "dir", "cat", "type", "head", "tail", 
        "wc", "grep", "find", "echo", "pwd", "cd"
    }
    
    def __init__(self, whitelist_path: str = None):
        """
        初始化命令执行器
        
        Args:
            whitelist_path: 白名单文件路径
        """
        if whitelist_path is None:
            base_dir = Path(__file__).parent.parent.parent
            whitelist_path = base_dir / ".allowed-commands"
        
        self.whitelist_path = Path(whitelist_path)
        self.session_allowed: Set[str] = set()
        self._ensure_whitelist_exists()
    
    def _ensure_whitelist_exists(self):
        """确保白名单文件存在"""
        if not self.whitelist_path.exists():
            with open(self.whitelist_path, 'w', encoding='utf-8') as f:
                f.write("# 允许执行的命令列表（每行一个）\n")
                for cmd in sorted(self.DEFAULT_ALLOWED):
                    f.write(f"{cmd}\n")
    
    def _load_whitelist(self) -> Set[str]:
        """加载白名单"""
        allowed = self.DEFAULT_ALLOWED.copy()
        
        try:
            with open(self.whitelist_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        allowed.add(line)
        except FileNotFoundError:
            pass
        
        return allowed
    
    def _add_to_whitelist(self, command: str):
        """将命令添加到白名单"""
        with open(self.whitelist_path, 'a', encoding='utf-8') as f:
            f.write(f"{command}\n")
    
    def _get_base_command(self, command: str) -> str:
        """提取命令的基础部分"""
        # 处理路径形式的命令
        parts = command.strip().split()
        if not parts:
            return ""
        
        base = parts[0]
        # 提取可执行文件名
        if '/' in base or '\\' in base:
            base = Path(base).name
        
        return base
    
    def is_allowed(self, command: str) -> bool:
        """检查命令是否在白名单中"""
        base_cmd = self._get_base_command(command)
        allowed = self._load_whitelist()
        return base_cmd in allowed or base_cmd in self.session_allowed
    
    def request_permission(self, command: str) -> Tuple[bool, bool]:
        """
        请求执行权限
        
        Args:
            command: 要执行的命令
            
        Returns:
            (是否允许, 是否永久信任)
        """
        base_cmd = self._get_base_command(command)
        
        print(f"\n⚠️  检测到非白名单命令: {base_cmd}")
        print(f"完整命令: {command}")
        print("\n请选择:")
        print("  1. 允许本次执行")
        print("  2. 拒绝执行")
        print("  3. 本次会话信任此命令")
        print("  4. 永久信任此命令（写入白名单）")
        
        while True:
            choice = input("\n请输入选项 [1/2/3/4]: ").strip()
            
            if choice == "1":
                return True, False
            elif choice == "2":
                return False, False
            elif choice == "3":
                self.session_allowed.add(base_cmd)
                return True, False
            elif choice == "4":
                self._add_to_whitelist(base_cmd)
                return True, True
            else:
                print("无效选项，请重新输入")
    
    def execute(
        self, 
        command: str, 
        cwd: str = None,
        timeout: int = 60,
        check_permission: bool = True
    ) -> Tuple[int, str, str]:
        """
        执行命令
        
        Args:
            command: 要执行的命令
            cwd: 工作目录
            timeout: 超时时间（秒）
            check_permission: 是否检查权限
            
        Returns:
            (返回码, 标准输出, 标准错误)
        """
        # 检查权限
        if check_permission and not self.is_allowed(command):
            allowed, _ = self.request_permission(command)
            if not allowed:
                return -1, "", "命令被用户拒绝执行"
        
        try:
            # Windows 使用 shell=True
            result = subprocess.run(
                command,
                shell=True,
                cwd=cwd,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            return result.returncode, result.stdout, result.stderr
            
        except subprocess.TimeoutExpired:
            return -1, "", f"命令执行超时（{timeout}秒）"
        except Exception as e:
            return -1, "", str(e)
    
    def execute_safe(self, command: str, cwd: str = None) -> Tuple[bool, str]:
        """
        安全执行命令（简化返回）
        
        Args:
            command: 要执行的命令
            cwd: 工作目录
            
        Returns:
            (是否成功, 输出内容或错误信息)
        """
        code, stdout, stderr = self.execute(command, cwd)
        
        if code == 0:
            return True, stdout
        else:
            return False, stderr or stdout or "命令执行失败"
