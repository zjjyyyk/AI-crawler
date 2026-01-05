"""
问答处理器
"""

import re
from typing import List, Dict, Any

from ..core.llm import LLMClient
from ..core.index import IndexManager
from ..utils.display import Display


class AskHandler:
    """问答处理器"""
    
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
    
    def handle(self, question: str) -> str:
        """
        处理问答请求
        
        Args:
            question: 用户问题
            
        Returns:
            回答内容
        """
        self.display.print_status("正在搜索本地数据索引...")
        
        # 提取关键词
        keywords = self._extract_keywords(question)
        self.display.print_status(f"搜索关键词: {', '.join(keywords)}")
        
        # 搜索数据集
        results = self.index.search(keywords)
        
        # 根据结果数量处理
        if len(results) == 0:
            answer = "未在本地索引中找到相关数据集信息。请先使用 crawl 命令爬取相关数据。"
            self.display.print_answer(question, answer)
            return answer
        
        elif len(results) == 1:
            # 单个结果，直接基于信息回答
            answer = self._answer_single(question, results[0])
            self.display.print_answer(question, answer, results)
            return answer
        
        else:
            # 多个结果，汇总回答
            answer = self._answer_multiple(question, results)
            self.display.print_answer(question, answer, results)
            return answer
    
    def _extract_keywords(self, question: str) -> List[str]:
        """从问题中提取关键词"""
        # 停用词
        stop_words = {
            '的', '是', '在', '有', '和', '与', '了', '吗', '呢', '吧',
            '什么', '多少', '哪些', '怎么', '如何',
            'the', 'a', 'an', 'is', 'are', 'of', 'to', 'for', 'in', 'on',
            'what', 'how', 'which', 'where', 'when', 'who', 'why',
            'many', 'much', 'does', 'do', 'have', 'has'
        }
        
        # 分词（简单按空格和标点分割）
        words = re.findall(r'[a-zA-Z0-9\u4e00-\u9fa5]+', question.lower())
        
        # 过滤停用词和单字符
        keywords = [w for w in words if w not in stop_words and len(w) > 1]
        
        return keywords
    
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
