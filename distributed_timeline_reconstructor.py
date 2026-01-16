#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分布式时序重构脚本

功能：读取混乱的日志流，重建事件的正确因果顺序，并识别系统异常。

作者：AI Assistant
日期：2026-01-16
"""

import json
import os
from collections import defaultdict, deque
from typing import Dict, List, Set, Any, Optional, Tuple


# 日志必需字段定义
REQUIRED_FIELDS = ['log_id', 'trace_id', 'node_id', 'event_type', 
                   'timestamp_ms', 'logical_clock', 'payload', 'causal_ref']

# 有效的事件类型
VALID_EVENT_TYPES = {'INIT', 'PROCESS', 'END'}


def validate_log(log: Any) -> Tuple[bool, Optional[str]]:
    """
    验证单条日志的格式是否有效
    
    参数:
        log: 待验证的日志对象
        
    返回:
        (是否有效, 日志ID或None)
    """
    # 检查是否为字典类型
    if not isinstance(log, dict):
        return False, None
    
    # 检查log_id是否存在且为字符串
    log_id = log.get('log_id')
    if not isinstance(log_id, str) or not log_id:
        return False, None
    
    # 检查所有必需字段是否存在
    for field in REQUIRED_FIELDS:
        if field not in log:
            return False, log_id
    
    # 检查trace_id是否为有效字符串
    if not isinstance(log.get('trace_id'), str) or not log.get('trace_id'):
        return False, log_id
    
    # 检查node_id是否为有效字符串
    if not isinstance(log.get('node_id'), str):
        return False, log_id
    
    # 检查event_type是否有效
    if log.get('event_type') not in VALID_EVENT_TYPES:
        return False, log_id
    
    # 检查timestamp_ms是否为有效数字
    if not isinstance(log.get('timestamp_ms'), (int, float)):
        return False, log_id
    
    # 检查logical_clock是否为有效数字
    if not isinstance(log.get('logical_clock'), (int, float)):
        return False, log_id
    
    # 检查payload是否为字典类型（对象）
    if not isinstance(log.get('payload'), dict):
        return False, log_id
    
    # 检查causal_ref（可以为null或字符串）
    causal_ref = log.get('causal_ref')
    if causal_ref is not None and not isinstance(causal_ref, str):
        return False, log_id
    
    return True, log_id


def build_log_index(valid_logs: List[Dict]) -> Dict[str, Dict]:
    """
    构建日志ID到日志对象的索引
    
    参数:
        valid_logs: 有效日志列表
        
    返回:
        log_id -> log对象的字典
    """
    return {log['log_id']: log for log in valid_logs}


def group_by_trace(valid_logs: List[Dict]) -> Dict[str, List[Dict]]:
    """
    按trace_id对日志进行分组
    
    参数:
        valid_logs: 有效日志列表
        
    返回:
        trace_id -> 日志列表的字典
    """
    traces = defaultdict(list)
    for log in valid_logs:
        traces[log['trace_id']].append(log)
    return dict(traces)


def detect_orphaned_logs(valid_logs: List[Dict], log_index: Dict[str, Dict]) -> Set[str]:
    """
    检测孤儿日志（causal_ref指向不存在的log_id）
    
    参数:
        valid_logs: 有效日志列表
        log_index: log_id索引
        
    返回:
        孤儿日志的log_id集合
    """
    orphaned = set()
    for log in valid_logs:
        causal_ref = log.get('causal_ref')
        if causal_ref is not None and causal_ref not in log_index:
            orphaned.add(log['log_id'])
    return orphaned


def detect_clock_skew(log: Dict, parent_log: Dict) -> bool:
    """
    检测时钟漂移事件（子事件的时间戳早于父事件）
    
    参数:
        log: 当前日志
        parent_log: 父日志（因果前驱）
        
    返回:
        是否存在时钟漂移
    """
    return log['timestamp_ms'] < parent_log['timestamp_ms']


def check_trace_completeness(trace_logs: List[Dict], log_index: Dict[str, Dict]) -> Tuple[bool, str]:
    """
    检查调用链的完整性
    
    参数:
        trace_logs: 同一trace下的所有日志
        log_index: 全局log_id索引
        
    返回:
        (是否完整, 失败原因)
    """
    # 检查是否有INIT事件
    has_init = any(log['event_type'] == 'INIT' for log in trace_logs)
    if not has_init:
        return False, "missing_init"
    
    # 检查是否有END事件
    has_end = any(log['event_type'] == 'END' for log in trace_logs)
    if not has_end:
        return False, "missing_end"
    
    # 构建trace内部的log_id集合
    trace_log_ids = {log['log_id'] for log in trace_logs}
    
    # 检查因果链是否完整（所有causal_ref都在trace内部或全局存在）
    for log in trace_logs:
        causal_ref = log.get('causal_ref')
        if causal_ref is not None:
            # 如果引用的日志不存在于全局索引中，说明因果链断裂
            if causal_ref not in log_index:
                return False, "broken_causal_chain"
    
    return True, "complete"


def topological_sort_trace(trace_logs: List[Dict], log_index: Dict[str, Dict]) -> List[Dict]:
    """
    对单个trace内的日志进行拓扑排序
    
    参数:
        trace_logs: 同一trace下的所有日志
        log_index: 全局log_id索引
        
    返回:
        按因果顺序排列的日志列表
    """
    # 构建trace内的log_id集合
    trace_log_ids = {log['log_id'] for log in trace_logs}
    
    # 构建邻接表和入度表
    # 如果 A.causal_ref == B.log_id，则 B -> A（B是A的前驱）
    in_degree = {log['log_id']: 0 for log in trace_logs}
    graph = defaultdict(list)  # parent -> children
    
    for log in trace_logs:
        causal_ref = log.get('causal_ref')
        if causal_ref is not None and causal_ref in trace_log_ids:
            graph[causal_ref].append(log['log_id'])
            in_degree[log['log_id']] += 1
    
    # Kahn算法进行拓扑排序
    # 对于同层级的节点，按logical_clock和timestamp_ms排序
    queue = []
    for log in trace_logs:
        if in_degree[log['log_id']] == 0:
            queue.append(log)
    
    # 按logical_clock和timestamp_ms排序队列
    queue.sort(key=lambda x: (x['logical_clock'], x['timestamp_ms']))
    queue = deque(queue)
    
    sorted_logs = []
    log_dict = {log['log_id']: log for log in trace_logs}
    
    while queue:
        # 从队列中取出所有入度为0的节点，按顺序处理
        current = queue.popleft()
        sorted_logs.append(current)
        
        # 获取当前节点的所有后继
        children = graph[current['log_id']]
        next_batch = []
        
        for child_id in children:
            in_degree[child_id] -= 1
            if in_degree[child_id] == 0:
                next_batch.append(log_dict[child_id])
        
        # 对新的入度为0的节点按logical_clock和timestamp_ms排序后加入队列
        next_batch.sort(key=lambda x: (x['logical_clock'], x['timestamp_ms']))
        for log in next_batch:
            queue.append(log)
    
    return sorted_logs


def process_logs(input_file: str, output_file: str) -> None:
    """
    主处理函数：读取日志文件，进行时序重构和异常检测
    
    参数:
        input_file: 输入文件路径
        output_file: 输出文件路径
    """
    print("正在读取输入文件...")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    raw_logs = data.get('raw_logs', [])
    system_config = data.get('system_config', {})
    max_clock_drift_ms = system_config.get('max_clock_drift_ms', 5000)
    
    print(f"读取完成，共 {len(raw_logs)} 条原始日志")
    
    # 第一步：验证日志格式，识别malformed_logs
    print("正在验证日志格式...")
    valid_logs = []
    malformed_logs = []
    
    for log in raw_logs:
        is_valid, log_id = validate_log(log)
        if is_valid:
            valid_logs.append(log)
        else:
            if log_id:
                malformed_logs.append(log_id)
    
    print(f"格式验证完成：有效日志 {len(valid_logs)} 条，格式错误日志 {len(malformed_logs)} 条")
    
    # 第二步：构建日志索引
    print("正在构建日志索引...")
    log_index = build_log_index(valid_logs)
    
    # 第三步：检测孤儿日志
    print("正在检测孤儿日志...")
    orphaned_log_ids = detect_orphaned_logs(valid_logs, log_index)
    print(f"孤儿日志数量：{len(orphaned_log_ids)}")
    
    # 第四步：按trace_id分组
    print("正在按trace_id分组...")
    traces = group_by_trace(valid_logs)
    print(f"共 {len(traces)} 个调用链")
    
    # 第五步：检测调用链完整性和时钟漂移
    print("正在检测调用链完整性...")
    corrupted_traces = []
    complete_traces = {}
    clock_skew_count = 0
    
    for trace_id, trace_logs in traces.items():
        is_complete, reason = check_trace_completeness(trace_logs, log_index)
        if is_complete:
            complete_traces[trace_id] = trace_logs
        else:
            corrupted_traces.append(trace_id)
    
    print(f"完整调用链：{len(complete_traces)} 个，异常调用链：{len(corrupted_traces)} 个")
    
    # 第六步：对完整调用链进行拓扑排序，并检测时钟漂移
    print("正在进行拓扑排序...")
    sorted_timeline = []
    
    # 首先收集所有完整trace的排序结果
    all_sorted_traces = []
    for trace_id, trace_logs in complete_traces.items():
        sorted_trace = topological_sort_trace(trace_logs, log_index)
        all_sorted_traces.append((trace_id, sorted_trace))
        
        # 检测时钟漂移
        for log in sorted_trace:
            causal_ref = log.get('causal_ref')
            if causal_ref is not None and causal_ref in log_index:
                parent_log = log_index[causal_ref]
                if detect_clock_skew(log, parent_log):
                    clock_skew_count += 1
    
    # 按trace中第一个日志的时间戳排序所有trace，然后合并
    all_sorted_traces.sort(key=lambda x: (
        min(log['timestamp_ms'] for log in x[1]) if x[1] else float('inf')
    ))
    
    for trace_id, sorted_trace in all_sorted_traces:
        sorted_timeline.extend(sorted_trace)
    
    print(f"时钟漂移事件数量：{clock_skew_count}")
    
    # 第七步：构建输出结果
    print("正在构建输出结果...")
    output = {
        "sorted_timeline": sorted_timeline,
        "anomaly_report": {
            "corrupted_traces": sorted(corrupted_traces),
            "orphaned_logs_count": len(orphaned_log_ids),
            "clock_skew_events_count": clock_skew_count,
            "malformed_logs": sorted(malformed_logs)
        }
    }
    
    # 确保输出目录存在
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 写入输出文件
    print(f"正在写入输出文件：{output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print("处理完成！")
    print(f"\n===== 处理结果摘要 =====")
    print(f"原始日志总数：{len(raw_logs)}")
    print(f"有效日志数量：{len(valid_logs)}")
    print(f"排序后时间线日志数量：{len(sorted_timeline)}")
    print(f"格式错误日志数量：{len(malformed_logs)}")
    print(f"异常调用链数量：{len(corrupted_traces)}")
    print(f"孤儿日志数量：{len(orphaned_log_ids)}")
    print(f"时钟漂移事件数量：{clock_skew_count}")


def main():
    """
    主函数入口
    """
    # 设置输入输出路径
    input_file = os.path.join('input', 'input.json')
    output_file = os.path.join('output', 'output.json')
    
    # 检查输入文件是否存在
    if not os.path.exists(input_file):
        print(f"错误：输入文件不存在：{input_file}")
        return
    
    # 执行处理
    process_logs(input_file, output_file)


if __name__ == '__main__':
    main()
