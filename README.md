# competition

本仓库用于竞赛相关内容的集中管理与协作。

## 目录说明
- `input/`：输入数据目录，放置待处理的 JSON 文件
- `output/`：输出数据目录，程序运行后生成结果 JSON 文件
- `main.py`：日志聚合统计程序

## 使用说明
- 运行环境：Python 3.8 及以上
- 运行命令：`python main.py`
- 输出规则：读取 `input/` 下所有 `.json` 文件，结果以同名文件写入 `output/`

