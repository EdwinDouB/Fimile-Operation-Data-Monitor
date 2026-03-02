1. Feature Overview

提供一个 Streamlit 页面，用于：

用户通过 上传 Excel/CSV 或 粘贴 Tracking ID 获取一组 Tracking IDs

对 Tracking IDs 做清洗、去重，并统计数量

点击按钮后按 Tracking ID 调用代码内配置的 API获取数据

按 DATA_CONTRACT.md 整理字段 + 计算派生字段

页面预览结果并下载导出文件

导出排序必须与输入 Tracking ID 的顺序一致（以首次出现顺序为准）

2. Input (Tracking IDs)
2.1 输入方式

必须支持三种方式：

上传 .xlsx（Excel）

上传 .csv

文本框粘贴（多行 / 逗号 / 空格分隔均可）

2.2 Tracking ID 规范化

系统必须对输入做：

trim（去首尾空格）

去除空行

统一大小写策略（如果你已有规则就写死：比如保持原样或转大写）

去重：只保留首次出现的 ID，且保留原始输入顺序

2.3 统计展示

页面必须展示：

input_count：清洗后总条数（含重复）

unique_count：去重后条数

duplicate_count = input_count - unique_count

（可选）重复的 Tracking ID 列表（或数量即可）

3. API Configuration (No User Input)

API Base URL / endpoint / auth（token 等）来自现有代码配置（例如常量、config 文件或环境变量），页面不提供输入框

Streamlit UI 中不展示 API 地址（避免误操作与泄露）

4. API Call Behavior
4.1 触发时机

仅在用户点击按钮（例如 Fetch / Export）后才发起请求。

4.2 请求策略

若 API 支持批量：按 API 限制进行分批（batch size 可配置）

若不支持批量：逐个请求

需要输出进度（已完成/总数）

4.3 错误处理与可观测性

对失败的 tracking_id 记录失败原因（至少是 status code/异常信息）

页面展示：

成功数量

失败数量

失败 tracking_id 列表（可下载或可复制）

5. Data Processing & Calculations

API 返回的数据必须按 docs/DATA_CONTRACT.md 进行字段整理与计算

不允许新增未在 DATA_CONTRACT 中定义的输出字段（除非 DATA_CONTRACT 明确允许扩展区）

缺字段/空值处理遵循 DATA_CONTRACT 规则

6. Output Requirements
6.1 排序（强制）

导出数据必须满足：

输出行顺序 严格等于用户输入 tracking_id 的顺序

如果用户输入里有重复：以去重后的首次出现顺序为准

API 返回顺序不可信，不能用于输出排序

6.2 展示与下载

页面展示结果预览（前 N 行）

提供下载按钮：

CSV 或 Excel（按你项目既定格式）

文件命名规则：export_YYYYMMDD_HHMMSS.csv/xlsx

7. Acceptance Criteria

粘贴 10 个 ID，其中 2 个重复：显示 unique=8，duplicate=2

导出文件顺序与输入顺序一致

API 部分失败：失败 ID 在页面可见；成功数据仍可导出（失败行按规则空缺或标记）

输出字段与 DATA_CONTRACT 完全一致