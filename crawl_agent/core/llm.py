"""
LLM 客户端封装
"""

import os
import json
import time
from typing import Optional
from openai import OpenAI
from dotenv import load_dotenv


class LLMClient:
    """LLM 客户端，封装对 qwen-flash 的调用"""
    
    def __init__(self, model: str = None, api_key: str = None, base_url: str = None):
        load_dotenv()
        
        self.model = model or os.getenv("LLM_MODEL", "qwen-flash")
        self.api_key = api_key or os.getenv("LLM_API_KEY") or os.getenv("DASHSCOPE_API_KEY")
        self.base_url = base_url or os.getenv("LLM_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
        
        if not self.api_key:
            raise ValueError("未配置 API Key，请设置环境变量 LLM_API_KEY 或 DASHSCOPE_API_KEY")
        
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )
        
        self.max_retries = 3
        self.temperature = 0.1
    
    def chat(self, system: str, user: str, json_mode: bool = False) -> str:
        """
        发送聊天请求
        
        Args:
            system: 系统提示
            user: 用户消息
            json_mode: 是否要求返回 JSON 格式
            
        Returns:
            LLM 的回复文本
        """
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user}
        ]
        
        for attempt in range(self.max_retries):
            try:
                kwargs = {
                    "model": self.model,
                    "messages": messages,
                    "temperature": self.temperature,
                }
                
                if json_mode:
                    kwargs["response_format"] = {"type": "json_object"}
                
                response = self.client.chat.completions.create(**kwargs)
                return response.choices[0].message.content
                
            except Exception as e:
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt  # 指数退避
                    time.sleep(wait_time)
                else:
                    raise RuntimeError(f"LLM 调用失败: {e}")
    
    def chat_json(self, system: str, user: str) -> dict:
        """
        发送聊天请求并解析 JSON 响应
        
        Args:
            system: 系统提示
            user: 用户消息
            
        Returns:
            解析后的 JSON 字典
        """
        response = self.chat(system, user, json_mode=True)
        
        # 尝试解析 JSON
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            # 尝试提取 JSON 块
            import re
            json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(1))
            
            # 尝试找到 {} 包裹的内容
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            
            raise ValueError(f"无法解析 LLM 返回的 JSON: {response[:200]}")
