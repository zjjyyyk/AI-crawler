#!/bin/bash

# crawl-agent 安装脚本

set -e

echo "======================================"
echo "  crawl-agent 安装脚本"
echo "======================================"
echo ""

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 打印函数
print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

# 1. 检查 Python 版本
echo "检查 Python 版本..."
if command -v python3 &> /dev/null; then
    PYTHON_CMD=python3
elif command -v python &> /dev/null; then
    PYTHON_CMD=python
else
    print_error "未找到 Python，请先安装 Python 3.10+"
    exit 1
fi

PYTHON_VERSION=$($PYTHON_CMD -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
PYTHON_MAJOR=$($PYTHON_CMD -c 'import sys; print(sys.version_info.major)')
PYTHON_MINOR=$($PYTHON_CMD -c 'import sys; print(sys.version_info.minor)')

if [ "$PYTHON_MAJOR" -lt 3 ] || ([ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 10 ]); then
    print_error "Python 版本过低: $PYTHON_VERSION，需要 3.10+"
    exit 1
fi

print_status "Python 版本: $PYTHON_VERSION"

# 2. 检查 pip
echo "检查 pip..."
if ! $PYTHON_CMD -m pip --version &> /dev/null; then
    print_error "未找到 pip，请先安装 pip"
    exit 1
fi
print_status "pip 已安装"

# 3. 创建虚拟环境
echo ""
echo "创建虚拟环境..."
if [ ! -d "venv" ]; then
    $PYTHON_CMD -m venv venv
    print_status "虚拟环境已创建"
else
    print_warning "虚拟环境已存在，跳过创建"
fi

# 4. 激活虚拟环境
echo "激活虚拟环境..."
if [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]] || [[ "$OSTYPE" == "cygwin" ]]; then
    source venv/Scripts/activate
else
    source venv/bin/activate
fi
print_status "虚拟环境已激活"

# 5. 安装依赖
echo ""
echo "安装依赖..."
pip install -r requirements.txt -q
print_status "依赖安装完成"

# 6. 安装 crawl-agent
echo ""
echo "安装 crawl-agent..."
pip install -e . -q
print_status "crawl-agent 安装完成"

# 7. 创建目录结构
echo ""
echo "创建目录结构..."
mkdir -p data/datasets
print_status "目录结构已创建"

# 8. 创建初始文件
if [ ! -f "data/index.json" ]; then
    echo '{"datasets": []}' > data/index.json
    print_status "index.json 已创建"
fi

if [ ! -f "data/crawl_history.json" ]; then
    echo '{"visited_urls": [], "last_updated": null}' > data/crawl_history.json
    print_status "crawl_history.json 已创建"
fi

# 9. 配置 API Key
echo ""
if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        cp .env.example .env
    fi
    
    echo "请配置你的 API Key:"
    read -p "请输入 DashScope API Key (或按回车跳过): " API_KEY
    
    if [ ! -z "$API_KEY" ]; then
        if [[ "$OSTYPE" == "darwin"* ]]; then
            sed -i '' "s/your_api_key_here/$API_KEY/" .env
        else
            sed -i "s/your_api_key_here/$API_KEY/" .env
        fi
        print_status "API Key 已配置"
    else
        print_warning "请稍后手动编辑 .env 文件配置 API Key"
    fi
else
    print_warning ".env 文件已存在，跳过配置"
fi

# 10. 完成
echo ""
echo "======================================"
echo -e "${GREEN}安装完成！${NC}"
echo "======================================"
echo ""
echo "使用方法:"
echo "  1. 激活虚拟环境:"
if [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]] || [[ "$OSTYPE" == "cygwin" ]]; then
    echo "     source venv/Scripts/activate"
else
    echo "     source venv/bin/activate"
fi
echo ""
echo "  2. 运行命令:"
echo "     crawl-agent crawl \"从 https://example.com 爬取数据\""
echo "     crawl-agent ask \"数据集有多少节点？\""
echo "     crawl-agent manage \"列出所有数据集\""
echo ""
echo "  3. 如果 crawl-agent 命令不可用，可以使用:"
echo "     python -m crawl_agent.cli <command> <prompt>"
echo ""
