"""
终端输出美化
"""

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich.markdown import Markdown
from rich.syntax import Syntax
from typing import Optional, List, Dict, Any


class Display:
    """终端美化输出工具"""
    
    def __init__(self):
        self.console = Console()
    
    def print_status(self, message: str):
        """打印蓝色状态信息"""
        self.console.print(f"[blue]ℹ {message}[/blue]")
    
    def print_success(self, message: str):
        """打印绿色成功信息"""
        self.console.print(f"[green]✓ {message}[/green]")
    
    def print_error(self, message: str):
        """打印红色错误信息"""
        self.console.print(f"[red]✗ {message}[/red]")
    
    def print_warning(self, message: str):
        """打印黄色警告信息"""
        self.console.print(f"[yellow]⚠ {message}[/yellow]")
    
    def print_result(self, title: str, content: str, border_style: str = "green"):
        """打印结果面板"""
        panel = Panel(
            content,
            title=title,
            border_style=border_style,
            expand=False
        )
        self.console.print(panel)
    
    def print_json(self, data: dict, title: str = None):
        """打印 JSON 数据"""
        import json
        json_str = json.dumps(data, ensure_ascii=False, indent=2)
        
        if title:
            self.console.print(f"\n[bold]{title}[/bold]")
        
        syntax = Syntax(json_str, "json", theme="monokai", line_numbers=False)
        self.console.print(syntax)
    
    def print_table(self, title: str, headers: List[str], rows: List[List[str]]):
        """打印表格"""
        table = Table(title=title, show_header=True, header_style="bold cyan")
        
        for header in headers:
            table.add_column(header)
        
        for row in rows:
            table.add_row(*row)
        
        self.console.print(table)
    
    def print_datasets(self, datasets: List[dict]):
        """打印数据集列表"""
        if not datasets:
            self.print_warning("未找到数据集")
            return
        
        table = Table(title="数据集列表", show_header=True, header_style="bold cyan")
        table.add_column("ID", style="dim")
        table.add_column("名称", style="green")
        table.add_column("描述")
        table.add_column("路径", style="blue")
        
        for ds in datasets:
            desc = ds.get("description", "")
            if len(desc) > 50:
                desc = desc[:50] + "..."
            
            table.add_row(
                ds.get("id", ""),
                ds.get("name", ""),
                desc,
                ds.get("local_path", "")
            )
        
        self.console.print(table)
    
    def confirm(self, message: str) -> bool:
        """确认操作"""
        self.console.print(f"\n[yellow]{message}[/yellow]")
        response = input("输入 y 确认，其他取消: ").strip().lower()
        return response == 'y' or response == 'yes'
    
    def progress_bar(self, description: str = "处理中"):
        """创建进度条上下文"""
        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=self.console
        )
    
    def print_crawl_summary(self, summary: dict):
        """打印爬取摘要"""
        self.console.print("\n")
        
        panel_content = f"""
[bold]爬取完成![/bold]

• 访问页面: [cyan]{summary.get('pages_visited', 0)}[/cyan]
• 下载资源: [cyan]{summary.get('resources_downloaded', 0)}[/cyan]
• 数据集数: [cyan]{summary.get('datasets_added', 0)}[/cyan]
• 保存路径: [blue]{summary.get('save_path', '')}[/blue]
• 耗时: [cyan]{summary.get('duration', 0):.1f}s[/cyan]
"""
        
        if summary.get('errors'):
            panel_content += f"\n[red]错误数: {len(summary['errors'])}[/red]"
        
        self.print_result("爬取摘要", panel_content.strip())
    
    def print_answer(self, question: str, answer: str, sources: List[dict] = None):
        """打印问答结果"""
        self.console.print("\n")
        self.console.print(f"[bold cyan]问题:[/bold cyan] {question}\n")
        
        self.print_result("回答", answer)
        
        if sources:
            self.console.print("\n[dim]相关数据集:[/dim]")
            for src in sources[:3]:
                self.console.print(f"  • [blue]{src.get('name', '')}[/blue] - {src.get('local_path', '')}")


# 单例实例
display = Display()
