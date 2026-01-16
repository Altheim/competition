# competition

本仓库用于竞赛相关内容的集中管理与协作。

## 目录说明
- `input/`：输入数据目录，放置待处理的 JSON 文件
- `output/`：输出数据目录，程序运行后生成结果 JSON 文件
- `main.py`：日志聚合统计程序

## 使用说明
- 运行环境：Python 3.8 及以上
- 运行命令：
  - `python main.py`：读取 `input/` 下所有 `.json` 文件
  - `python main.py input`：读取指定目录下所有 `.json` 文件
  - `python main.py input/input.json`：读取指定文件
- 输出规则：结果写入 `output/`，文件名为“输入文件名 + `-output` 后缀”
- stdin 规则：当传入参数不是有效文件或目录时，读取 stdin，输出文件名基于该参数生成

