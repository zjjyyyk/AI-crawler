"""
处理器模块
"""

from .crawl import CrawlHandler
from .ask import AskHandler
from .manage import ManageHandler

__all__ = ["CrawlHandler", "AskHandler", "ManageHandler"]
