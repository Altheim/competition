# competition

本仓库用于竞赛相关内容的集中管理与协作。

## 目录说明
- `input/`：输入数据目录，放置待处理的 JSON 文件
- `output/`：输出数据目录，程序运行后生成结果 JSON 文件
- `distributed_timeline_reconstructor.py`：分布式时序重构脚本

## 分布式时序重构

### 功能说明
读取混乱的分布式系统日志流，重建事件的正确因果顺序，并识别系统异常。

### 核心功能
1. **日志格式验证**：检测并过滤格式错误的日志（malformed_logs）
2. **因果关系重建**：基于 `causal_ref` 字段构建事件因果图
3. **调用链完整性检测**：识别缺少 INIT/END 事件或因果链断裂的异常调用链（corrupted_traces）
4. **孤儿日志检测**：识别 `causal_ref` 指向不存在日志的孤儿事件（orphaned_logs）
5. **时钟漂移检测**：识别子事件时间戳早于父事件的时钟漂移现象（clock_skew_events）
6. **拓扑排序**：对完整调用链进行因果顺序排序

### 使用说明
- 运行环境：Python 3.8 及以上
- 运行命令：`python distributed_timeline_reconstructor.py`
- 输入文件：`input/input.json`
- 输出文件：`output/output.json`

### 输出格式
```json
{
  "sorted_timeline": [...],
  "anomaly_report": {
    "corrupted_traces": [],
    "orphaned_logs_count": 0,
    "clock_skew_events_count": 0,
    "malformed_logs": []
  }
}
```

| 字段 | 说明 |
|------|------|
| sorted_timeline | 按正确因果顺序排列的有效日志（仅包含完整调用链） |
| corrupted_traces | 异常调用链的 trace_id 列表 |
| orphaned_logs_count | 孤儿日志数量 |
| clock_skew_events_count | 时钟漂移事件数量 |
| malformed_logs | 格式错误日志的 log_id 列表 |
