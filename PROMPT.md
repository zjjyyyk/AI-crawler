# AI-爬虫+DataManager 开发任务书

## 项目概述

开发一个命令行工具 `crawl-agent`，通过自然语言指令完成网页数据爬取、知识问答和数据管理三大功能。

**核心设计原则**：
- LLM 只负责决策（理解意图、分析内容、做出选择）
- 代码负责执行（网络请求、文件操作、数据存储）
- 状态管理用文件系统和 JSON，不使用数据库
- 不使用 Agent 框架，LLM 就是普通的函数调用

**LLM 配置**：
- 默认模型：qwen-flash
- 只需要基础 chat 能力和 JSON 格式输出
- 不使用 Function Call，LLM 返回 JSON 后由代码解析执行

---

## 使用方式

```bash
# 安装
./build.sh

# 爬取数据
crawl-agent crawl "从 https://snap.stanford.edu/data/ 爬取所有社交网络数据集，保存到 dataset/snap"

# 知识问答
crawl-agent ask "DBLP 数据集有多少节点和边？"

# 数据管理
crawl-agent manage "把 snap 的数据移动到 /data/graphs/"
```

---

## 项目结构

```
crawl-agent/
├── crawl_agent/
│   ├── __init__.py
│   ├── cli.py                 # 命令行入口
│   ├── handlers/
│   │   ├── __init__.py
│   │   ├── crawl.py           # 爬取处理器
│   │   ├── ask.py             # 问答处理器
│   │   └── manage.py          # 管理处理器
│   ├── core/
│   │   ├── __init__.py
│   │   ├── llm.py             # LLM 客户端封装
│   │   ├── web.py             # 网络请求客户端
│   │   ├── terminal.py        # 命令执行器（带权限控制）
│   │   └── index.py           # 数据索引管理
│   └── utils/
│       ├── __init__.py
│       ├── display.py         # 终端输出美化
│       └── html_cleaner.py    # HTML 清洗工具
├── data/
│   ├── index.json             # 数据集索引
│   ├── crawl_history.json     # 爬取历史
│   └── datasets/              # 数据集存储目录
├── .allowed-commands          # 命令白名单
├── .env                       # 环境变量（API Key 等）
├── build.sh                   # 安装脚本
├── requirements.txt
└── README.md
```

---

## 模块设计

### 1. CLI 入口 (cli.py)

**功能**：解析命令行参数，路由到对应处理器

**输入格式**：`crawl-agent <command> "<prompt>"`
- command：crawl | ask | manage
- prompt：用户的自然语言指令

**行为**：
1. 解析命令和 prompt
2. 加载配置（从 .env 读取 API Key 等）
3. 根据 command 调用对应 Handler
4. 输出结果

---

### 2. 爬取处理器 (handlers/crawl.py)

**功能**：根据用户指令爬取网站数据

**整体流程**：

```
用户 prompt
    │
    ▼
[LLM] 解析意图 → 提取 url, save_path, criteria, max_depth
    │
    ▼
递归爬取（从 root_url 开始，depth=0）
    │
    ├─► 检查：已访问过？超过深度限制？→ 是则跳过
    │
    ▼
[代码] 获取 HTML
    │
    ▼
[代码] 清洗 HTML → 得到正文文本 + 链接列表
    │
    ▼
[代码] 内容过长则截断（保留前 8000 字符）
    │
    ▼
[LLM] 分析页面 → 返回要下载的资源 + 要跟进的子链接
    │
    ▼
[代码] 下载资源文件，生成 meta.json
    │
    ▼
[代码] 对子链接递归执行上述流程（depth+1）
    │
    ▼
[代码] 更新 data/index.json
    │
    ▼
输出结果摘要
```

**LLM 调用点 1 - 解析意图**

输入：用户原始 prompt

输出 JSON：
```json
{
  "url": "根 URL",
  "save_path": "保存路径，默认 data/datasets/{域名}",
  "criteria": "筛选条件",
  "max_depth": 2
}
```

**LLM 调用点 2 - 分析页面**

输入：
- 清洗后的页面文本（最多 8000 字符）
- 链接列表（最多 50 个）
- 筛选条件

输出 JSON：
```json
{
  "resources": [
    {
      "name": "数据集名称",
      "description": "描述，含统计信息",
      "download_urls": ["下载链接"],
      "properties": {
        "nodes": 123,
        "edges": 456,
        "directed": false
      }
    }
  ],
  "follow_links": ["值得跟进的子链接"]
}
```

要求：
- download_urls 必须是实际的下载链接（.gz, .zip, .csv 等）
- follow_links 只选可能有更多数据的页面
- properties 只填页面中明确存在的信息
- 不编造链接

**HTML 清洗规则**：
- 移除：script, style, nav, header, footer, aside, form, iframe
- 移除 HTML 注释
- 提取文本，保留基本换行结构
- 去除连续空行

**截断策略**（内容超 8000 字符时）：
- 优先保留表格内容
- 优先保留列表内容
- 保留链接及其上下文
- 兜底：保留前 8000 字符

**防循环机制**：
- 内存中维护 visited_urls 集合
- URL 标准化：去末尾斜杠、去锚点、去查询参数
- 爬取结束后持久化到 crawl_history.json

---

### 3. 问答处理器 (handlers/ask.py)

**功能**：从本地索引搜索并回答问题

**流程**：

```
用户问题
    │
    ▼
[代码] 加载 index.json
    │
    ▼
[代码] 关键词搜索 → 匹配 name, description, tags
    │
    ▼
结果数量判断
    │
    ├─► 0 条 → 返回"未找到相关信息"
    ├─► 1 条 → [LLM] 基于数据集信息回答
    └─► 多条 → [LLM] 汇总回答（200字内）
```

**搜索逻辑**：
1. 提取问题关键词，去除停用词
2. 在数据集的 name、description、tags、properties 中匹配
3. 评分：名称匹配 +10，其他匹配 +1
4. 返回前 10 条

**LLM 调用 - 组织答案**

输入：问题 + 检索到的数据集信息

输出要求：
- 以"已从本地索引检索到 [N] 条相关信息："开头
- 只用检索到的信息，不编造

---

### 4. 管理处理器 (handlers/manage.py)

**功能**：管理数据文件（移动、删除、复制）

**流程**：

```
用户指令
    │
    ▼
[LLM] 解析意图 → action, source, target
    │
    ▼
[代码] 解析源路径
    │     具体路径 → 直接用
    │     模糊描述 → 查 index.json 匹配
    │
    ▼
[代码] 显示预览，请求确认
    │
    ▼
[代码] 执行操作
    │
    ▼
[代码] 更新 index.json
    │
    ▼
输出结果
```

**LLM 调用 - 解析意图**

输出 JSON：
```json
{
  "action": "move | delete | copy | list",
  "source": "源路径或描述",
  "target": "目标路径"
}
```

**确认机制**：
破坏性操作前必须显示预览并等待用户确认。

---

### 5. LLM 客户端 (core/llm.py)

**功能**：封装 LLM 调用

**配置**：
- 默认模型：qwen-flash
- API Key 从环境变量读取

**特性**：
- 统一接口：chat(system, user) → str
- 自动重试：3 次，指数退避
- 低温度：0.1
- 支持 JSON 格式输出

---

### 6. 网络客户端 (core/web.py)

**功能**：HTTP 请求和文件下载

**特性**：
- 请求间隔：默认 1 秒
- 自动重试：3 次
- 下载显示进度条
- 超时：请求 30 秒，下载 5 分钟

**方法**：
- fetch(url) → HTML 或 None
- download(url, save_path) → bool

---

### 7. 命令执行器 (core/terminal.py)

**功能**：执行 shell 命令，带权限控制

**机制**：
- 白名单文件 .allowed-commands
- 默认允许：python, pip, ls, cat, head, tail, wc, grep
- 非白名单命令需交互确认

**确认选项**：
1. 允许本次
2. 拒绝
3. 本次会话信任
4. 永久信任（写入白名单）

注：当前 manage 功能用 Python shutil 实现，暂不需要 shell 命令。此模块预留扩展。

---

### 8. 数据索引 (core/index.py)

**功能**：管理 data/index.json

**数据结构**：

```json
{
  "datasets": [
    {
      "id": "snap/com-dblp",
      "name": "com-DBLP",
      "source_url": "https://...",
      "local_path": "data/datasets/snap/com-dblp",
      "description": "...",
      "properties": {
        "nodes": 317080,
        "edges": 1049866,
        "directed": false
      },
      "tags": ["social-network"],
      "crawl_time": "2024-01-15T10:30:00Z",
      "files": [
        {"name": "xxx.gz", "size": 4215438}
      ]
    }
  ]
}
```

**方法**：
- get_all()
- search(keywords)
- add(dataset)
- update_path(old, new)
- delete(id)

---

### 9. 输出美化 (utils/display.py)

**功能**：终端美化输出

使用 rich 库：
- print_status(msg) → 蓝色状态
- print_success(msg) → 绿色 ✓
- print_error(msg) → 红色 ✗
- print_warning(msg) → 黄色 ⚠
- print_result(title, content) → 结果面板
- 进度条

---

### 10. HTML 清洗 (utils/html_cleaner.py)

**功能**：清洗 HTML

**输入**：原始 HTML、base_url

**输出**：
- clean_text：清洗后文本
- links：绝对路径链接列表

**步骤**：
1. 解析 HTML
2. 移除噪音标签
3. 提取链接，转绝对路径，可选过滤站外
4. 提取文本
5. 清理空白

---

## build.sh

**功能**：
1. 检查 Python 3.10+
2. 检查 pip, git
3. 创建虚拟环境
4. 安装依赖
5. 配置 API Key（写入 .env）
6. 创建目录结构
7. 创建默认 .allowed-commands
8. 设置命令别名（可选）

**依赖** (requirements.txt)：
- openai（或对应的 qwen SDK）
- requests
- beautifulsoup4
- rich
- python-dotenv

---

## 错误处理

1. **网络错误**：重试后跳过，继续其他任务
2. **LLM 格式错误**：重试 1 次，失败则跳过
3. **文件操作错误**：显示错误，询问是否继续
4. **Ctrl+C**：保存当前状态，优雅退出

---

## 开发顺序

1. core/（llm, web, index）
2. utils/（display, html_cleaner）
3. handlers/crawl.py
4. handlers/ask.py
5. handlers/manage.py
6. cli.py
7. build.sh
8. 测试

---

## 不需要实现

- Agent 框架（LangGraph 等）
- 向量数据库
- Function Call
- 多线程/异步
- Web UI