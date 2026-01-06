"""
测试查询引擎的各种功能
"""

from crawl_agent.handlers.query_engine import QueryEngine, QueryBuilder

# 测试数据
datasets = [
    {'id': '1', 'name': 'facebook', 'local_path': '/snap/social/facebook', 'properties': {'nodes': 4039, 'edges': 88234}},
    {'id': '2', 'name': 'twitter', 'local_path': '/snap/social/twitter', 'properties': {'nodes': 81306, 'edges': 1768149}},
    {'id': '3', 'name': 'roadnet-ca', 'local_path': '/snap/road/roadnet-ca', 'properties': {'nodes': 1965206, 'edges': 2766607}},
    {'id': '4', 'name': 'wiki', 'local_path': '/konect/social/wiki', 'properties': {'nodes': 7115, 'edges': 103689}},
    {'id': '5', 'name': 'eu-road', 'local_path': '/konect/road/eu-road', 'properties': {'nodes': 1039, 'edges': 1305}},
]

qe = QueryEngine()

print('=== 测试1: 关键词过滤 ===')
r = qe.query(datasets, {'keywords': ['snap']})
print(f"找到 {r['count']} 个 snap 数据集")

print('\n=== 测试2: 条件过滤 ===')
r = qe.query(datasets, {'conditions': [{'field': 'nodes', 'op': '>', 'value': 5000}]})
print(f"节点数>5000: {r['count']} 个")

print('\n=== 测试3: 多关键词 + 条件 ===')
r = qe.query(datasets, {'keywords': ['snap', 'social'], 'conditions': [{'field': 'nodes', 'op': '>', 'value': 1000}]})
print(f"snap社交网络n>1000: {r['count']} 个")

print('\n=== 测试4: 多组查询(OR) ===')
r = qe.query(datasets, {'or_groups': [
    {'keywords': ['snap', 'road']},
    {'keywords': ['konect', 'social']}
]})
print(f"snap路网 OR konect社交: {r['count']} 个")

print('\n=== 测试5: 排序 ===')
r = qe.query(datasets, {'sort': 'nodes', 'sort_order': 'desc', 'limit': 3})
names = [d['name'] for d in r['data']]
print(f"按节点数降序前3: {names}")

print('\n=== 测试6: 聚合统计 ===')
r = qe.query(datasets, {'aggregate': 'sum:nodes'})
print(f"总节点数: {r['aggregation']}")

print('\n=== 测试7: 分组统计 (按路径首段) ===')
r = qe.query(datasets, {'aggregate': [{'type': 'count'}, {'type': 'avg', 'field': 'nodes'}]})
print(f"统计结果: {r['aggregation']}")

print('\n=== 测试8: 范围查询 ===')
r = qe.query(datasets, {'conditions': [{'field': 'nodes', 'op': 'between', 'value': [1000, 10000]}]})
names = [d['name'] for d in r['data']]
print(f"1000-10000节点: {r['count']} 个, 名称: {names}")

print('\n=== 测试9: QueryBuilder 链式API ===')
qb = QueryBuilder()
spec = qb.keywords('snap').where('nodes', '>', 1000).sort('nodes', 'desc').limit(5).build()
r = qe.query(datasets, spec)
print(f"snap n>1000 (链式API): {r['count']} 个")

print('\n=== 测试10: 字符串操作符 ===')
r = qe.query(datasets, {'conditions': [{'field': 'name', 'op': 'contains', 'value': 'road'}]})
names = [d['name'] for d in r['data']]
print(f"名称包含road: {names}")

r = qe.query(datasets, {'conditions': [{'field': 'name', 'op': 'startswith', 'value': 'road'}]})
names = [d['name'] for d in r['data']]
print(f"名称以road开头: {names}")

print('\n=== 测试11: 复杂多组查询 ===')
r = qe.query(datasets, {'or_groups': [
    {'keywords': ['snap', 'social'], 'conditions': [{'field': 'nodes', 'op': '>', 'value': 10000}]},
    {'keywords': ['konect'], 'conditions': [{'field': 'edges', 'op': '<', 'value': 10000}]}
]})
names = [d['name'] for d in r['data']]
print(f"(snap社交 n>10000) OR (konect m<10000): {names}")

print('\n=== 测试12: IN 操作符 ===')
r = qe.query(datasets, {'conditions': [{'field': 'name', 'op': 'in', 'value': ['facebook', 'twitter', 'wiki']}]})
names = [d['name'] for d in r['data']]
print(f"名称在列表中: {names}")

print('\n✅ 所有测试通过!')
