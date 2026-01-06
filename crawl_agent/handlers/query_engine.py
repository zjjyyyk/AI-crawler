"""
智能查询引擎 - 支持复杂的数据集查询
"""

import re
from typing import List, Dict, Any, Optional, Union
from datetime import datetime


class QueryEngine:
    """
    数据集智能查询引擎
    
    支持的功能：
    1. 关键词过滤（AND/OR）
    2. 条件过滤（数值比较、字符串匹配、范围、空值检查）
    3. 多组查询（OR 组合）
    4. 排序（单字段/多字段，升序/降序）
    5. 分页（limit/offset）
    6. 聚合统计（count/sum/avg/min/max/group）
    7. 字段选择
    8. 去重
    """
    
    # 字段名别名映射
    FIELD_ALIASES = {
        # 节点数
        "n": "nodes",
        "nodes": "nodes",
        "node": "nodes",
        "vertices": "nodes",
        "v": "nodes",
        "节点": "nodes",
        "节点数": "nodes",
        
        # 边数
        "m": "edges",
        "e": "edges",
        "edges": "edges",
        "edge": "edges",
        "links": "edges",
        "边": "edges",
        "边数": "edges",
        
        # 文件大小
        "size": "size",
        "filesize": "size",
        "file_size": "size",
        "大小": "size",
        
        # 名称
        "name": "name",
        "名称": "name",
        "名字": "name",
        
        # 描述
        "desc": "description",
        "description": "description",
        "描述": "description",
        
        # 来源
        "source": "source_url",
        "url": "source_url",
        "source_url": "source_url",
        "来源": "source_url",
        
        # 路径
        "path": "local_path",
        "local_path": "local_path",
        "路径": "local_path",
        
        # 爬取时间
        "time": "crawl_time",
        "crawl_time": "crawl_time",
        "date": "crawl_time",
        "时间": "crawl_time",
        
        # 标签
        "tags": "tags",
        "tag": "tags",
        "标签": "tags",
        
        # 格式
        "format": "format",
        "type": "format",
        "格式": "format",
        "类型": "format",
    }
    
    # 操作符别名
    OP_ALIASES = {
        ">": ">",
        "gt": ">",
        "greater": ">",
        "大于": ">",
        
        ">=": ">=",
        "gte": ">=",
        "ge": ">=",
        "大于等于": ">=",
        
        "<": "<",
        "lt": "<",
        "less": "<",
        "小于": "<",
        
        "<=": "<=",
        "lte": "<=",
        "le": "<=",
        "小于等于": "<=",
        
        "=": "==",
        "==": "==",
        "eq": "==",
        "equals": "==",
        "等于": "==",
        
        "!=": "!=",
        "<>": "!=",
        "ne": "!=",
        "neq": "!=",
        "not_equals": "!=",
        "不等于": "!=",
        
        "contains": "contains",
        "include": "contains",
        "has": "contains",
        "包含": "contains",
        
        "not_contains": "not_contains",
        "exclude": "not_contains",
        "不包含": "not_contains",
        
        "startswith": "startswith",
        "starts_with": "startswith",
        "prefix": "startswith",
        "以开头": "startswith",
        
        "endswith": "endswith",
        "ends_with": "endswith",
        "suffix": "endswith",
        "以结尾": "endswith",
        
        "regex": "regex",
        "match": "regex",
        "正则": "regex",
        
        "between": "between",
        "range": "between",
        "在范围": "between",
        
        "in": "in",
        "in_list": "in",
        "在列表": "in",
        
        "not_in": "not_in",
        "不在列表": "not_in",
        
        "is_null": "is_null",
        "null": "is_null",
        "empty": "is_null",
        "为空": "is_null",
        
        "is_not_null": "is_not_null",
        "not_null": "is_not_null",
        "exists": "is_not_null",
        "不为空": "is_not_null",
    }
    
    def __init__(self):
        pass
    
    def query(self, datasets: List[Dict], query_spec: Dict) -> Dict:
        """
        执行查询
        
        Args:
            datasets: 原始数据集列表
            query_spec: 查询规格，支持以下字段：
                - keywords: 关键词（字符串或列表）
                - keywords_mode: "and" | "or"（默认 and）
                - conditions: 条件列表
                - or_groups: 多组查询（OR 关系）
                - sort: 排序规格
                - limit: 返回数量限制
                - offset: 偏移量
                - aggregate: 聚合操作
                - distinct: 是否去重
                - fields: 返回字段列表
                
        Returns:
            {
                "success": True,
                "data": [...],  # 结果数据
                "count": n,     # 结果数量
                "total": m,     # 总数量
                "aggregation": {...}  # 聚合结果（如有）
            }
        """
        total = len(datasets)
        result = datasets.copy()
        aggregation = None
        
        # 1. 多组查询（OR 关系）
        or_groups = query_spec.get("or_groups") or query_spec.get("queries")
        if or_groups:
            result = self._apply_or_groups(result, or_groups)
        else:
            # 2. 关键词过滤
            keywords = query_spec.get("keywords") or query_spec.get("source")
            keywords_mode = query_spec.get("keywords_mode", "and")
            if keywords:
                result = self._filter_by_keywords(result, keywords, keywords_mode)
            
            # 3. 条件过滤
            conditions = query_spec.get("conditions")
            if conditions:
                result = self._filter_by_conditions(result, conditions)
        
        # 4. 去重
        if query_spec.get("distinct"):
            result = self._distinct(result)
        
        # 5. 聚合（在排序/分页前执行）
        aggregate = query_spec.get("aggregate")
        if aggregate:
            aggregation = self._aggregate(result, aggregate)
        
        # 6. 排序
        sort = query_spec.get("sort") or query_spec.get("sort_by")
        if sort:
            result = self._sort(result, sort, query_spec.get("sort_order", "asc"))
        
        # 7. 分页
        offset = query_spec.get("offset", 0)
        limit = query_spec.get("limit")
        if offset > 0:
            result = result[offset:]
        if limit:
            result = result[:limit]
        
        # 8. 字段选择
        fields = query_spec.get("fields")
        if fields:
            result = self._select_fields(result, fields)
        
        return {
            "success": True,
            "data": result,
            "count": len(result),
            "total": total,
            "aggregation": aggregation
        }
    
    def _normalize_field(self, field: str) -> str:
        """标准化字段名"""
        return self.FIELD_ALIASES.get(field.lower(), field.lower())
    
    def _normalize_op(self, op: str) -> str:
        """标准化操作符"""
        return self.OP_ALIASES.get(op.lower(), op.lower())
    
    def _get_field_value(self, ds: Dict, field: str) -> Any:
        """获取数据集的字段值"""
        field = self._normalize_field(field)
        
        # 优先从 properties 获取
        props = ds.get("properties", {})
        if field in props:
            return props[field]
        
        # 然后从顶层获取
        if field in ds:
            return ds[field]
        
        # 特殊处理
        if field == "nodes":
            return props.get("nodes") or props.get("n")
        if field == "edges":
            return props.get("edges") or props.get("m") or props.get("e")
        
        return None
    
    def _get_searchable_text(self, ds: Dict) -> str:
        """获取数据集的可搜索文本"""
        parts = [
            ds.get("local_path", ""),
            ds.get("name", ""),
            ds.get("source_url", ""),
            ds.get("description", ""),
            " ".join(ds.get("tags", [])),
        ]
        return " ".join(parts).lower()
    
    def _filter_by_keywords(self, datasets: List[Dict], keywords: Union[str, List], mode: str = "and") -> List[Dict]:
        """按关键词过滤"""
        if isinstance(keywords, str):
            keywords = [keywords]
        keywords = [k.lower() for k in keywords if k]
        
        if not keywords:
            return datasets
        
        result = []
        for ds in datasets:
            text = self._get_searchable_text(ds)
            
            if mode == "or":
                # 任一关键词匹配
                if any(kw in text for kw in keywords):
                    result.append(ds)
            else:
                # 所有关键词都必须匹配
                if all(kw in text for kw in keywords):
                    result.append(ds)
        
        return result
    
    def _filter_by_conditions(self, datasets: List[Dict], conditions: List[Dict]) -> List[Dict]:
        """按条件过滤"""
        result = []
        
        for ds in datasets:
            match = True
            for cond in conditions:
                if not self._check_condition(ds, cond):
                    match = False
                    break
            if match:
                result.append(ds)
        
        return result
    
    def _check_condition(self, ds: Dict, cond: Dict) -> bool:
        """检查单个条件"""
        field = cond.get("field", "")
        op = self._normalize_op(cond.get("op", "=="))
        value = cond.get("value")
        
        actual = self._get_field_value(ds, field)
        
        return self._compare(actual, op, value)
    
    def _compare(self, actual: Any, op: str, expected: Any) -> bool:
        """执行比较操作"""
        # 空值检查
        if op == "is_null":
            return actual is None or actual == "" or actual == []
        if op == "is_not_null":
            return actual is not None and actual != "" and actual != []
        
        # 其他操作需要 actual 非空
        if actual is None:
            return False
        
        try:
            # 数值比较
            if op == ">":
                return float(actual) > float(expected)
            elif op == ">=":
                return float(actual) >= float(expected)
            elif op == "<":
                return float(actual) < float(expected)
            elif op == "<=":
                return float(actual) <= float(expected)
            elif op == "==":
                if isinstance(actual, (int, float)) and not isinstance(actual, bool):
                    return float(actual) == float(expected)
                return str(actual).lower() == str(expected).lower()
            elif op == "!=":
                if isinstance(actual, (int, float)) and not isinstance(actual, bool):
                    return float(actual) != float(expected)
                return str(actual).lower() != str(expected).lower()
            
            # 字符串操作
            elif op == "contains":
                return str(expected).lower() in str(actual).lower()
            elif op == "not_contains":
                return str(expected).lower() not in str(actual).lower()
            elif op == "startswith":
                return str(actual).lower().startswith(str(expected).lower())
            elif op == "endswith":
                return str(actual).lower().endswith(str(expected).lower())
            elif op == "regex":
                return bool(re.search(str(expected), str(actual), re.IGNORECASE))
            
            # 范围操作
            elif op == "between":
                if isinstance(expected, (list, tuple)) and len(expected) >= 2:
                    return float(expected[0]) <= float(actual) <= float(expected[1])
                return False
            
            # 列表操作
            elif op == "in":
                if isinstance(expected, (list, tuple)):
                    return actual in expected or str(actual).lower() in [str(e).lower() for e in expected]
                return False
            elif op == "not_in":
                if isinstance(expected, (list, tuple)):
                    return actual not in expected and str(actual).lower() not in [str(e).lower() for e in expected]
                return True
            
            else:
                return False
                
        except (ValueError, TypeError):
            return False
    
    def _apply_or_groups(self, datasets: List[Dict], groups: List[Dict]) -> List[Dict]:
        """应用多组查询（OR 关系）"""
        result_set = set()
        result_map = {}
        
        for group in groups:
            keywords = group.get("keywords", [])
            conditions = group.get("conditions", [])
            keywords_mode = group.get("keywords_mode", "and")
            
            # 先按关键词过滤
            filtered = self._filter_by_keywords(datasets, keywords, keywords_mode)
            
            # 再按条件过滤
            if conditions:
                filtered = self._filter_by_conditions(filtered, conditions)
            
            # 添加到结果集（去重）
            for ds in filtered:
                ds_id = ds.get("id") or ds.get("local_path") or id(ds)
                if ds_id not in result_set:
                    result_set.add(ds_id)
                    result_map[ds_id] = ds
        
        return list(result_map.values())
    
    def _sort(self, datasets: List[Dict], sort_spec: Union[str, List, Dict], default_order: str = "asc") -> List[Dict]:
        """排序"""
        # 标准化排序规格
        if isinstance(sort_spec, str):
            # 简单字段名
            sort_fields = [(sort_spec, default_order)]
        elif isinstance(sort_spec, dict):
            # {"field": "nodes", "order": "desc"}
            sort_fields = [(sort_spec.get("field"), sort_spec.get("order", default_order))]
        elif isinstance(sort_spec, list):
            # [{"field": "nodes", "order": "desc"}, "name"] 或 ["nodes", "-edges"]
            sort_fields = []
            for item in sort_spec:
                if isinstance(item, str):
                    if item.startswith("-"):
                        sort_fields.append((item[1:], "desc"))
                    else:
                        sort_fields.append((item, "asc"))
                elif isinstance(item, dict):
                    sort_fields.append((item.get("field"), item.get("order", "asc")))
        else:
            return datasets
        
        # 执行排序（多字段排序，从后往前）
        result = datasets.copy()
        for field, order in reversed(sort_fields):
            if not field:
                continue
            reverse = order.lower() in ("desc", "descending", "d", "-1")
            result.sort(key=lambda x: self._sort_key(x, field), reverse=reverse)
        
        return result
    
    def _sort_key(self, ds: Dict, field: str) -> Any:
        """获取排序键"""
        value = self._get_field_value(ds, field)
        if value is None:
            # None 值排在最后
            return (1, "")
        if isinstance(value, (int, float)):
            return (0, value)
        return (0, str(value).lower())
    
    def _distinct(self, datasets: List[Dict]) -> List[Dict]:
        """去重"""
        seen = set()
        result = []
        for ds in datasets:
            ds_id = ds.get("id") or ds.get("local_path") or id(ds)
            if ds_id not in seen:
                seen.add(ds_id)
                result.append(ds)
        return result
    
    def _aggregate(self, datasets: List[Dict], aggregate_spec: Union[str, Dict, List]) -> Dict:
        """聚合统计"""
        result = {}
        
        # 标准化聚合规格
        if isinstance(aggregate_spec, str):
            # "count" 或 "sum:nodes" 或 "group:source"
            if ":" in aggregate_spec:
                parts = aggregate_spec.split(":", 1)
                specs = [{"type": parts[0], "field": parts[1]}]
            else:
                specs = [{"type": aggregate_spec}]
        elif isinstance(aggregate_spec, dict):
            specs = [aggregate_spec]
        elif isinstance(aggregate_spec, list):
            specs = aggregate_spec
        else:
            return result
        
        for spec in specs:
            agg_type = spec.get("type", "count").lower()
            field = spec.get("field")
            alias = spec.get("alias", f"{agg_type}_{field}" if field else agg_type)
            
            if agg_type == "count":
                result[alias] = len(datasets)
                
            elif agg_type == "sum" and field:
                values = [self._get_field_value(ds, field) for ds in datasets]
                values = [v for v in values if v is not None]
                result[alias] = sum(float(v) for v in values) if values else 0
                
            elif agg_type == "avg" and field:
                values = [self._get_field_value(ds, field) for ds in datasets]
                values = [v for v in values if v is not None]
                result[alias] = sum(float(v) for v in values) / len(values) if values else 0
                
            elif agg_type == "min" and field:
                values = [self._get_field_value(ds, field) for ds in datasets]
                values = [v for v in values if v is not None]
                result[alias] = min(float(v) for v in values) if values else None
                
            elif agg_type == "max" and field:
                values = [self._get_field_value(ds, field) for ds in datasets]
                values = [v for v in values if v is not None]
                result[alias] = max(float(v) for v in values) if values else None
                
            elif agg_type == "group" and field:
                groups = {}
                for ds in datasets:
                    key = self._get_field_value(ds, field)
                    key = str(key) if key is not None else "(null)"
                    if key not in groups:
                        groups[key] = []
                    groups[key].append(ds)
                result[alias] = {k: len(v) for k, v in groups.items()}
                
            elif agg_type == "distinct" and field:
                values = [self._get_field_value(ds, field) for ds in datasets]
                result[alias] = len(set(v for v in values if v is not None))
        
        return result
    
    def _select_fields(self, datasets: List[Dict], fields: List[str]) -> List[Dict]:
        """选择返回字段"""
        fields = [self._normalize_field(f) for f in fields]
        result = []
        for ds in datasets:
            item = {}
            for field in fields:
                item[field] = self._get_field_value(ds, field)
            result.append(item)
        return result


# 查询构建器（便捷 API）
class QueryBuilder:
    """查询构建器 - 链式 API"""
    
    def __init__(self):
        self._spec = {}
    
    def keywords(self, *args, mode: str = "and") -> "QueryBuilder":
        """添加关键词"""
        self._spec["keywords"] = list(args)
        self._spec["keywords_mode"] = mode
        return self
    
    def where(self, field: str, op: str, value: Any) -> "QueryBuilder":
        """添加条件"""
        if "conditions" not in self._spec:
            self._spec["conditions"] = []
        self._spec["conditions"].append({"field": field, "op": op, "value": value})
        return self
    
    def or_group(self, keywords: List[str] = None, conditions: List[Dict] = None) -> "QueryBuilder":
        """添加 OR 组"""
        if "or_groups" not in self._spec:
            self._spec["or_groups"] = []
        self._spec["or_groups"].append({
            "keywords": keywords or [],
            "conditions": conditions or []
        })
        return self
    
    def sort(self, field: str, order: str = "asc") -> "QueryBuilder":
        """排序"""
        if "sort" not in self._spec:
            self._spec["sort"] = []
        self._spec["sort"].append({"field": field, "order": order})
        return self
    
    def limit(self, n: int) -> "QueryBuilder":
        """限制数量"""
        self._spec["limit"] = n
        return self
    
    def offset(self, n: int) -> "QueryBuilder":
        """偏移量"""
        self._spec["offset"] = n
        return self
    
    def aggregate(self, agg_type: str, field: str = None) -> "QueryBuilder":
        """聚合"""
        if "aggregate" not in self._spec:
            self._spec["aggregate"] = []
        self._spec["aggregate"].append({"type": agg_type, "field": field})
        return self
    
    def distinct(self) -> "QueryBuilder":
        """去重"""
        self._spec["distinct"] = True
        return self
    
    def fields(self, *args) -> "QueryBuilder":
        """选择字段"""
        self._spec["fields"] = list(args)
        return self
    
    def build(self) -> Dict:
        """构建查询规格"""
        return self._spec
    
    def execute(self, datasets: List[Dict]) -> Dict:
        """执行查询"""
        engine = QueryEngine()
        return engine.query(datasets, self._spec)
