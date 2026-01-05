@echo off
REM crawl-agent Windows 安装脚本

setlocal enabledelayedexpansion

echo ======================================
echo   crawl-agent 安装脚本
echo ======================================
echo.

REM 1. 检查 Python 版本
echo 检查 Python 版本...
where python >nul 2>&1
if %errorlevel% neq 0 (
    echo [X] 未找到 Python，请先安装 Python 3.10+
    echo 下载地址: https://www.python.org/downloads/
    pause
    exit /b 1
)

REM 获取 Python 版本
for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
echo [√] Python 版本: %PYTHON_VERSION%

REM 检查 Python 版本是否满足要求 (3.10+)
python -c "import sys; exit(0 if sys.version_info >= (3, 10) else 1)" 2>nul
if %errorlevel% neq 0 (
    echo [X] Python 版本过低，需要 3.10+
    pause
    exit /b 1
)

REM 2. 检查 pip
echo 检查 pip...
python -m pip --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [X] 未找到 pip，请先安装 pip
    pause
    exit /b 1
)
echo [√] pip 已安装

REM 3. 创建虚拟环境
echo.
echo 创建虚拟环境...
if not exist "venv" (
    python -m venv venv
    if %errorlevel% neq 0 (
        echo [X] 虚拟环境创建失败
        pause
        exit /b 1
    )
    echo [√] 虚拟环境已创建
) else (
    echo [!] 虚拟环境已存在，跳过创建
)

REM 4. 激活虚拟环境
echo 激活虚拟环境...
call venv\Scripts\activate.bat
if %errorlevel% neq 0 (
    echo [X] 虚拟环境激活失败
    pause
    exit /b 1
)
echo [√] 虚拟环境已激活

REM 5. 升级 pip
echo.
echo 升级 pip...
python -m pip install --upgrade pip -q

REM 6. 安装依赖
echo.
echo 安装依赖...
pip install -r requirements.txt -q
if %errorlevel% neq 0 (
    echo [X] 依赖安装失败
    pause
    exit /b 1
)
echo [√] 依赖安装完成

REM 7. 安装 crawl-agent
echo.
echo 安装 crawl-agent...
pip install -e . -q
if %errorlevel% neq 0 (
    echo [X] crawl-agent 安装失败
    pause
    exit /b 1
)
echo [√] crawl-agent 安装完成

REM 8. 创建目录结构
echo.
echo 创建目录结构...
if not exist "data" mkdir data
if not exist "data\datasets" mkdir data\datasets
echo [√] 目录结构已创建

REM 9. 创建初始文件
if not exist "data\index.json" (
    echo {"datasets": []}> data\index.json
    echo [√] index.json 已创建
)

if not exist "data\crawl_history.json" (
    echo {"visited_urls": [], "last_updated": null}> data\crawl_history.json
    echo [√] crawl_history.json 已创建
)

REM 10. 配置 API Key
echo.
if not exist ".env" (
    if exist ".env.example" (
        copy .env.example .env >nul
    )
    
    echo 请配置你的 API Key:
    set /p API_KEY="请输入 DashScope API Key (或按回车跳过): "
    
    if not "!API_KEY!"=="" (
        REM 使用 PowerShell 替换文件内容
        powershell -Command "(Get-Content .env) -replace 'your_api_key_here', '!API_KEY!' | Set-Content .env"
        echo [√] API Key 已配置
    ) else (
        echo [!] 请稍后手动编辑 .env 文件配置 API Key
    )
) else (
    echo [!] .env 文件已存在，跳过配置
)

REM 11. 完成
echo.
echo ======================================
echo 安装完成！
echo ======================================
echo.
echo 使用方法:
echo   1. 激活虚拟环境:
echo      venv\Scripts\activate
echo.
echo   2. 运行命令:
echo      crawl-agent crawl "从 https://example.com 爬取数据"
echo      crawl-agent ask "数据集有多少节点？"
echo      crawl-agent manage "列出所有数据集"
echo.
echo   3. 如果 crawl-agent 命令不可用，可以使用:
echo      python -m crawl_agent.cli crawl "指令"
echo.
echo 按任意键退出...
pause >nul
