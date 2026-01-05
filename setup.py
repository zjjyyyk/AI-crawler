"""
crawl-agent 安装配置
"""

from setuptools import setup, find_packages

setup(
    name="crawl-agent",
    version="1.0.0",
    description="基于 LLM 的智能爬虫命令行工具",
    author="User",
    python_requires=">=3.10",
    packages=find_packages(),
    install_requires=[
        "openai>=1.0.0",
        "requests>=2.28.0",
        "beautifulsoup4>=4.11.0",
        "rich>=13.0.0",
        "python-dotenv>=1.0.0",
    ],
    entry_points={
        "console_scripts": [
            "crawl-agent=crawl_agent.cli:main",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
