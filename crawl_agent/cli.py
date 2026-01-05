"""
命令行入口
"""

import sys
import argparse
from pathlib import Path

from dotenv import load_dotenv

from .handlers import CrawlHandler, AskHandler, ManageHandler
from .utils.display import Display


def main():
    """主入口函数"""
    # 加载环境变量
    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(env_path)
    
    # 创建显示器
    display = Display()
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(
        prog="crawl-agent",
        description="基于 LLM 的智能爬虫命令行工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  crawl-agent crawl "从 https://snap.stanford.edu/data/ 爬取社交网络数据集"
  crawl-agent ask "DBLP 数据集有多少节点和边？"
  crawl-agent manage "把 snap 的数据移动到 /data/graphs/"
        """
    )
    
    parser.add_argument(
        "command",
        choices=["crawl", "ask", "manage"],
        help="命令类型: crawl(爬取), ask(问答), manage(管理)"
    )
    
    parser.add_argument(
        "prompt",
        help="自然语言指令"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="显示详细输出"
    )
    
    # 处理无参数情况
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    
    args = parser.parse_args()
    
    try:
        # 根据命令路由到对应处理器
        if args.command == "crawl":
            display.print_status("启动爬取任务...")
            handler = CrawlHandler()
            result = handler.handle(args.prompt)
            
        elif args.command == "ask":
            display.print_status("处理问答请求...")
            handler = AskHandler()
            result = handler.handle(args.prompt)
            
        elif args.command == "manage":
            display.print_status("处理管理请求...")
            handler = ManageHandler()
            result = handler.handle(args.prompt)
        
    except KeyboardInterrupt:
        display.print_warning("\n用户中断操作")
        sys.exit(130)
    except ValueError as e:
        display.print_error(f"参数错误: {e}")
        sys.exit(1)
    except Exception as e:
        display.print_error(f"执行错误: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
