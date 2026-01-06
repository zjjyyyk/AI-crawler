"""
问答处理器
"""

import re
from typing import List, Dict, Any, Optional

from ..core.llm import LLMClient
from ..core.index import IndexManager
from ..utils.display import Display
from .query_engine import QueryEngine


class AskHandler:
    """问答处理器"""
    
    # 解析问题意图的系统提示
    PARSE_QUESTION_SYSTEM = """你是一个问题解析助手。分析用户关于数据集的问题，提取搜索关键词。

请返回 JSON 格式：
{
  "keywords": ["关键词1", "关键词2"],  // 用于搜索的关键词（英文）
  "question_type": "count | info | compare | stats",  // 问题类型
  "field": "nodes | edges | size | name"  // 如果是统计问题，统计哪个字段
}

问题类型说明：
- count: 统计数量（如"有多少个"、"一共有几个"）
- info: 查询具体信息（如"节点数是多少"）
- compare: 比较多个数据集
- stats: 统计分析（如"平均节点数"、"总边数"）

关键词提取规则：
1. 提取数据集来源/名称：snap, konect, networkrepository 等
2. 提取数据集类型：social(社交), road(路网), citation(引用), bio(生物) 等
3. 提取具体数据集名称：facebook, twitter, dblp 等
4. 去除无关词：数据集、网络、图、的、有、多少、一共 等

示例：
- "konect数据集一共有多少？" -> {"keywords": ["konect"], "question_type": "count"}
- "snap社交网络有哪些？" -> {"keywords": ["snap", "social"], "question_type": "count"}
- "facebook数据集有多少节点？" -> {"keywords": ["facebook"], "question_type": "info", "field": "nodes"}
- "konect路网的平均边数" -> {"keywords": ["konect", "road"], "question_type": "stats", "field": "edges"}"""
    
    # 回答问题的系统提示
    ANSWER_SYSTEM = """你是一个数据集问答助手。根据检索到的数据集信息回答用户问题。

规则：
1. 只使用检索到的信息回答，不要编造
2. 如果信息不足，如实告知
3. 回答简洁明了，200字以内
4. 如果有多个相关数据集，进行汇总比较"""

    def __init__(self):
        self.llm = LLMClient()
        self.index = IndexManager()
        self.display = Display()
        self.query_engine = QueryEngine()
    
    def handle(self, question: str) -> str:
        """
        处理问答请求
        
        Args:
            question: 用户问题
            
        Returns:
            回答内容
        """
        self.display.print_status("正在分析问题...")
        
        # 1. 解析问题意图
        parsed = self._parse_question(question)
        keywords = parsed.get("keywords", [])
        question_type = parsed.get("question_type", "info")
        field = parsed.get("field")
        
        self.display.print_status(f"搜索关键词: {', '.join(keywords)}")
        if question_type != "info":
            self.display.print_status(f"问题类型: {question_type}")
        
        # 2. 获取所有数据集
        all_datasets = self.index.get_all()
        
        # 3. 使用查询引擎过滤
        query_spec = {"keywords": keywords} if keywords else {}
        result = self.query_engine.query(all_datasets, query_spec)
        datasets = result.get("data", [])
        
        # 4. 根据问题类型生成回答
        if question_type == "count":
            answer = self._answer_count(question, datasets, keywords)
        elif question_type == "stats" and field:
            answer = self._answer_stats(question, datasets, field, keywords)
        elif len(datasets) == 0:
            answer = "未在本地索引中找到相关数据集信息。请先使用 crawl 命令爬取相关数据。"
        elif len(datasets) == 1:
            answer = self._answer_single(question, datasets[0])
        else:
            answer = self._answer_multiple(question, datasets)
        
        self.display.print_answer(question, answer, datasets if datasets else None)
        return answer
    
    def _parse_question(self, question: str) -> dict:
        """使用 LLM 解析问题"""
        try:
            result = self.llm.chat_json(
                self.PARSE_QUESTION_SYSTEM,
                f"问题: {question}"
            )
            return result
        except Exception:
            # 降级：使用简单关键词提取
            return {
                "keywords": self._extract_keywords_simple(question),
                "question_type": "info"
            }
    
    def _extract_keywords_simple(self, question: str) -> List[str]:
        """简单关键词提取（降级方案）"""
        # 已知的数据源和类型关键词
        known_keywords = {
            # 数据源
            'snap', 'konect', 'networkrepository', 'network repository',
            # 类型
            'social', 'road', 'citation', 'bio', 'communication',
            '社交', '路网', '引用', '生物', '通信',
            # 常见数据集
            'facebook', 'twitter', 'dblp', 'youtube', 'amazon', 'google',
        }
        
        question_lower = question.lower()
        found = []
        
        for kw in known_keywords:
            if kw in question_lower:
                # 翻译中文
                translations = {
                    '社交': 'social', '路网': 'road', '引用': 'citation',
                    '生物': 'bio', '通信': 'communication'
                }
                found.append(translations.get(kw, kw))
        
        return list(set(found)) if found else [question]
    
    def _answer_count(self, question: str, datasets: List[dict], keywords: List[str]) -> str:
        """回答数量统计问题"""
        count = len(datasets)
        kw_str = "、".join(keywords) if keywords else "所有"
        
        if count == 0:
            return f"未找到 {kw_str} 相关的数据集。请先使用 crawl 命令爬取。"
        elif count == 1:
            ds = datasets[0]
            return f"本地索引中有 1 个 {kw_str} 数据集：{ds.get('name', '未知')}。"
        else:
            # 列出前几个
            names = [ds.get('name', '未知') for ds in datasets[:5]]
            names_str = "、".join(names)
            if count > 5:
                names_str += f" 等"
            return f"本地索引中共有 {count} 个 {kw_str} 数据集，包括：{names_str}。"
    
    def _answer_stats(self, question: str, datasets: List[dict], field: str, keywords: List[str]) -> str:
        """回答统计分析问题"""
        if not datasets:
            kw_str = "、".join(keywords) if keywords else ""
            return f"未找到 {kw_str} 相关的数据集，无法进行统计。"
        
        # 收集字段值
        values = []
        for ds in datasets:
            props = ds.get("properties", {})
            val = props.get(field) or props.get(field[0])  # 尝试 nodes 或 n
            if val is not None:
                try:
                    values.append(float(val))
                except:
                    pass
        
        if not values:
            return f"找到 {len(datasets)} 个数据集，但没有 {field} 字段的信息。"
        
        # 计算统计值
        total = sum(values)
        avg = total / len(values)
        min_val = min(values)
        max_val = max(values)
        
        kw_str = "、".join(keywords) if keywords else "所有"
        field_name = {"nodes": "节点数", "edges": "边数", "size": "大小"}.get(field, field)
        
        return (
            f"在 {len(datasets)} 个 {kw_str} 数据集中（{len(values)} 个有 {field_name} 信息）：\n"
            f"• 总计: {total:,.0f}\n"
            f"• 平均: {avg:,.0f}\n"
            f"• 最小: {min_val:,.0f}\n"
            f"• 最大: {max_val:,.0f}"
        )
    
    def _format_dataset_info(self, dataset: dict) -> str:
        """格式化数据集信息"""
        info_parts = [
            f"名称: {dataset.get('name', 'unknown')}",
            f"描述: {dataset.get('description', '无')}",
            f"路径: {dataset.get('local_path', '无')}",
        ]
        
        props = dataset.get('properties', {})
        if props:
            if props.get('nodes'):
                info_parts.append(f"节点数: {props['nodes']}")
            if props.get('edges'):
                info_parts.append(f"边数: {props['edges']}")
            if props.get('directed') is not None:
                info_parts.append(f"有向图: {'是' if props['directed'] else '否'}")
        
        files = dataset.get('files', [])
        if files:
            file_names = [f['name'] for f in files]
            info_parts.append(f"文件: {', '.join(file_names)}")
        
        return '\n'.join(info_parts)
    
    def _answer_single(self, question: str, dataset: dict) -> str:
        """基于单个数据集回答"""
        dataset_info = self._format_dataset_info(dataset)
        
        user_prompt = f"""用户问题: {question}

检索到的数据集信息:
{dataset_info}

请基于以上信息回答用户问题。以"已从本地索引检索到 1 条相关信息："开头。"""

        try:
            answer = self.llm.chat(self.ANSWER_SYSTEM, user_prompt)
            return answer
        except Exception as e:
            # 降级：直接返回数据集信息
            return f"已从本地索引检索到 1 条相关信息：\n\n{dataset_info}"
    
    def _answer_multiple(self, question: str, datasets: List[dict]) -> str:
        """基于多个数据集回答"""
        datasets_info = []
        for i, ds in enumerate(datasets, 1):
            datasets_info.append(f"[数据集 {i}]\n{self._format_dataset_info(ds)}")
        
        all_info = '\n\n'.join(datasets_info)
        
        user_prompt = f"""用户问题: {question}

检索到的数据集信息:
{all_info}

请基于以上信息回答用户问题，进行汇总比较。以"已从本地索引检索到 {len(datasets)} 条相关信息："开头。"""

        try:
            answer = self.llm.chat(self.ANSWER_SYSTEM, user_prompt)
            return answer
        except Exception as e:
            # 降级：直接返回数据集信息
            return f"已从本地索引检索到 {len(datasets)} 条相关信息：\n\n{all_info}"
