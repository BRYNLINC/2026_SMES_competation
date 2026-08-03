# MI Challenge 计分说明

本文档说明当前 `ChallengeMI` 的计分统计方法、数据流转过程、关键函数入口，以及结果表中各字段的中文含义。

## 1. 相关代码与配置入口

- 计分主实现文件：`app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.py`
- 最终总分汇总入口：`app/ProcessHub/ProcessHub/bci_competition/task/BCICompetitionTaskPreliminary.py`（当前决赛主线仍复用该实现）
- 计分配置文件：`app/ProcessHub/ProcessHub/bci_competition/challenge/MI/ChallengeMI.yml`

## 2. 当前任务组织方式

当前任务按：

`task_id = exp_name + "_" + exp_task`

例如：

- `vme_left_vs_rest`
- `vme_right_vs_rest`
- `vmi_left_vs_rest`
- `vmi_right_vs_rest`

基准分从 `ChallengeMI.yml -> score_config -> task_baseline_score` 中读取，按上述 `task_id` 分开配置。

## 3. 计分整体流程

### 3.1 初始化阶段

`ChallengeMI.initial()`

- 读取 `ChallengeMI.yml`
- 读取 VirtualReceiver 配置
- 初始化日志与运行期上下文

`ChallengeMI.receive_algorithm_config()`

- 读取算法上报的模型信息
- 提取并缓存：
  - 使用通道数
  - 每类校准 trial 数
  - 模型大小
- 清空任务静态得分缓存
- 准备结果输出目录

### 3.2 单 trial 上报阶段

正常 trial 结果入口：

`ChallengeMI.receive_report()`

超时 trial 入口：

`ChallengeMI.timeout_trigger()`

二者都会最终构造成统一的 trial 记录，然后进入：

`ChallengeMI.__append_record_and_score()`

这个函数会完成以下工作：

1. 去重，防止同一个 trial 被重复记分
2. 将 trial 追加到内存记录列表 `__trial_record_list`
3. 调用 `__build_record_score_snapshot()` 生成该 trial 截止当前时刻的累计计分快照
4. 生成 `ScorePackageModel`，用于实时显示/上报
5. 增量落盘当前结果表

### 3.3 单 trial 计分阶段

单 trial 的核心计分入口：

`ChallengeMI.__build_record_score_snapshot(record)`

这个函数不是只给“本 trial”算一个瞬时分，而是计算：

- 当前任务截止这个 trial 的累计平均反应时间
- 当前任务截止这个 trial 的累计准确率
- 当前任务截止这个 trial 的累计准确率稳定性
- 当前任务截止这个 trial 的累计总分

也就是说，`{task_id}_score.csv` 中每一行都是“该 task 截止当前 trial 的累计状态”。

### 3.4 任务汇总阶段

任务汇总入口：

`ChallengeMI.__build_task_summary_dict()`

对于每个 `task_id`：

- 取这个任务最后一个 trial 的 `score_snapshot`
- 将它作为该任务的最终统计结果
- 同时统计每个被试的准确率与 trial 数
- 取该任务的 baseline 分
- 计算任务最终分和 baseline 裁剪后的分

### 3.5 总分汇总阶段

总分入口：

`BCICompetitionTaskPreliminary.__build_final_score_result(score_context)`

对每个任务执行：

`adjusted_task_score = task_score if task_score >= baseline_score else 0.0`

最终队伍总分：

`total_score = mean(adjusted_task_score for all tasks)`

代码中当前记录的公式字符串为：

`mean(last_trial_cumulative_score if last_trial_cumulative_score >= baseline_score else 0)`

## 4. 单 trial 计分逻辑

### 4.1 静态分

静态分每个任务只计算一次，并缓存在：

`ChallengeMI.__resolve_task_static_score_snapshot(task_id)`

包括三部分：

- 校准分 `calibration_score`
- 通道分 `channel_score`
- 模型大小分 `model_size_score`（仅在平台成功统计该队 `model_artifacts` 目录时启用）

这三部分在同一个任务的所有 trial 中保持不变。

### 4.2 动态分

动态分随 trial 推进不断更新，包括两部分：

- 累计平均反应时间得分
- 累计准确率得分

### 4.3 单 trial 的累计总分

`cumulative_score = calibration_score + channel_score + [model_size_score if enabled] + cumulative_avg_reaction_time_score + cumulative_accuracy_score`

注意：

- 这里的 `cumulative_score` 是“当前任务截止这个 trial 的累计分”
- 它不是单独这个 trial 的瞬时分
- 一个任务最终用于总分比较的 `task_score`，就是该任务最后一个 trial 的 `cumulative_score`
- 如果平台没有拿到该队 `model_artifacts` 目录统计结果，则模型大小项整项跳过，不按 0 分硬扣

## 5. 关键公式

### 5.1 累计准确率

在当前任务下，取截至当前 trial 的所有有效预测：

- 预测正确记为 `1`
- 预测错误记为 `0`

则：

`cumulative_accuracy = correct_count / total_count`

`cumulative_accuracy_percent = cumulative_accuracy * 100`

### 5.2 累计准确率标准差

当前不是直接对 `0/100` 做标准差，而是对“累计准确率历史序列”做标准差。

例如某任务前 5 个 trial 的正确性序列为：

`[1, 0, 1, 1, 0]`

则累计准确率历史序列为：

- 第 1 个 trial 后：`100`
- 第 2 个 trial 后：`50`
- 第 3 个 trial 后：`66.67`
- 第 4 个 trial 后：`75`
- 第 5 个 trial 后：`60`

然后使用总体标准差：

`sigma = pstdev(cumulative_accuracy_history_percent_list)`

对应代码：

`ChallengeMI.__build_record_score_snapshot()`

### 5.3 准确率得分

入口：

`ChallengeMI.__compute_accuracy_score(mu_accuracy_percent, sigma_accuracy_percent)`

公式：

`Sper = accuracy_score_max * max(0, (mu - lambda * sigma) / 100)`

当前默认配置来自 `ChallengeMI.yml`：

- `accuracy_score_max = 80.0`
- `accuracy_stability_penalty_lambda = 0.5`

并且最终会裁剪到 `[0, accuracy_score_max]`。

### 5.4 反应时间得分

入口：

`ChallengeMI.__compute_reaction_time_score(average_reaction_time_ms)`

公式：

`Stime = reaction_time_score_max * (1 - avg_rt_ms / reaction_time_reference_ms)`

再裁剪到 `[0, reaction_time_score_max]`。

当前默认配置：

- `reaction_time_score_max = 2.0`
- `reaction_time_reference_ms = 1000.0`

所以：

- 平均反应时间越短，得分越高
- 当平均反应时间大于等于参考时间时，该项得分为 `0`

### 5.5 通道分

入口：

`ChallengeMI.__compute_channel_score(channel_count)`

公式：

`Schannel = channel_score_max * (channel_reference_count - channel_count) / (channel_reference_count - 1)`

再裁剪到合法范围。

当前默认配置：

- `channel_score_max = 8.0`
- `channel_reference_count = 8`

结论：

- 通道越少，分数越高
- 使用 8 通道时该项通常为 0

### 5.6 校准分

入口：

`ChallengeMI.__compute_calibration_score(calibration_trials_per_class)`

公式：

`Scal = calibration_score_max * (1 - calibration_trials_per_class / calibration_reference_trials_per_class)`

当前默认配置：

- `calibration_score_max = 7.0`
- `calibration_reference_trials_per_class = 10`

结论：

- 校准 trial 越少，得分越高
- 每类校准 10 轮时，该项通常为 0

### 5.7 模型大小分

入口：

`ChallengeMI.__compute_model_size_score(model_size_mb)`

公式：

`Ssize = model_size_score_max * max(0, 1 - model_size_mb / model_size_reference_mb)`

模型大小来源：

- 不再信任算法端 `AlgorithmImplement` 自报的模型大小
- 由选手端不可修改的算法框架层直接统计该队 `model_artifacts` 目录总大小
- 裁判机侧只接收框架层上报的 `platform_model_size_mb`
- 如果框架层拿不到目录统计结果，则该项不参与计分
- 同一赛队所有 task 共用同一个 `model_size_mb`

当前默认配置：

- `model_size_score_max = 3.0`
- `model_size_reference_mb = 150.0`

结论：

- 模型越小，得分越高
- 模型大小大于等于参考值时，该项为 0
- 若平台未拿到目录大小，则不会因为平台缺少配置而给队伍额外扣分

## 6. 超时处理规则

超时入口：

`ChallengeMI.timeout_trigger()`

超时配置来自：

`ChallengeMI.yml -> strategy_config -> timeout_setting`

当前默认行为：

- 超时上限：`1s`
- 超时预测标签：`wrong`

超时 trial 的处理规则：

1. 该 trial 记为错误预测
2. `predict_label` 记为配置中的超时标签，默认 `wrong`
3. 反应时间按超时上限记为 `1000ms`
4. 因为 `1000ms == reaction_time_reference_ms(1000ms)`，所以反应时间得分为 `0`

## 7. 任务最终分与总分

### 7.1 任务最终分

任务最终分取该任务最后一个 trial 的：

`cumulative_score`

对应汇总中记为：

`task_score`

### 7.2 baseline 裁剪

baseline 从配置读取：

`ChallengeMI.yml -> score_config -> task_baseline_score`

每个任务独立比较：

`adjusted_task_score = task_score if task_score >= baseline_score else 0`

### 7.3 队伍总分

最终总分为所有任务的 `adjusted_task_score` 平均值：

`total_score = mean(adjusted_task_score list)`

注意：

- 没有达到 baseline 的任务，按 `0` 参与平均
- 未开始的任务如果仍在任务列表中，也会以 0 分形式进入最终平均
- `task_sequence` 由 `__resolve_configured_task_order()` 决定，通常会包含已运行任务和配置中的任务

## 8. 关键函数入口说明

### 8.1 ChallengeMI 侧

- `initial()`
  - 读取配置，初始化计分环境
- `receive_algorithm_config()`
  - 读取算法侧模型信息，决定静态分参数
- `receive_report()`
  - 正常 trial 上报入口
- `timeout_trigger()`
  - 超时 trial 上报入口
- `__build_trial_record()`
  - 将算法上报结果整理成统一 trial 记录
- `__append_record_and_score()`
  - 追加 trial，生成实时计分，触发增量落盘
- `__build_record_score_snapshot()`
  - 计算当前 trial 截止时刻的累计分
- `__build_task_summary_dict()`
  - 生成每个任务的最终汇总
- `__build_score_context()`
  - 生成供总分模块使用的上下文
- `__persist_trial_record_files()`
  - 落盘各任务逐 trial 结果表
- `__persist_score_result_file()`
  - 落盘队伍/任务/被试汇总表

### 8.2 当前决赛主线 task 侧

- `__build_final_score_result(score_context)`
  - 根据 task summary 计算 baseline 裁剪后的任务分与总分

## 9. 当前输出结果文件

当前结果目录：

`app/ProcessHub/ProcessHub/bci_competition/challenge/MI/result/<team_id>/`

当前主要输出：

- `team_score.csv`
- `task_summary.csv`
- `subject_task_summary.csv`
- `{task_id}_score.csv`

其中：

- `{task_id}` 例如 `vmi_left_vs_rest`
- 对应文件名示例：`vmi_left_vs_rest_score.csv`

## 10. 各结果表字段中文解释

### 10.1 team_score.csv

这是队伍级总览表，格式为 `item,value`。

| 字段 | 中文解释 |
| --- | --- |
| `team_id` | 队伍标识 |
| `record_count` | 当前累计记录到的 trial 总数 |
| `configured_task_count` | 当前纳入总分计算的任务数 |
| `started_task_count` | 已经实际开始并产生过 trial 的任务数 |
| `task_sequence` | 当前纳入汇总的任务顺序 |
| `started_task_sequence` | 当前已经开始的任务顺序 |
| `mean_accuracy_percent` | 各任务累计准确率百分比的平均值 |
| `mean_accuracy_score` | 各任务准确率得分的平均值 |
| `avg_reaction_time_ms` | 各任务累计平均反应时间的平均值，单位毫秒 |
| `mean_reaction_time_score` | 各任务反应时间得分的平均值 |
| `channel_count` | 当前算法使用的通道数 |
| `mean_channel_score` | 各任务通道得分的平均值 |
| `calibration_trials_per_class` | 当前算法每类使用的校准 trial 数 |
| `mean_calibration_score` | 各任务校准得分的平均值 |
| `model_size_mb` | 当前赛队由平台直接统计得到的 `model_artifacts` 目录大小，单位 MB；若未统计到则为空 |
| `mean_model_size_score` | 各任务模型大小得分的平均值 |
| `accuracy_stability_penalty_lambda` | 准确率稳定性惩罚系数 |
| `baseline_rule` | baseline 裁剪规则说明 |
| `total_score_formula` | 总分公式说明 |
| `total_score` | 队伍最终总分 |

### 10.2 task_summary.csv

这是任务级汇总表，每行一个任务。

| 字段 | 中文解释 |
| --- | --- |
| `team_id` | 队伍标识 |
| `task_name` | 任务唯一标识，即 `task_id` |
| `exp_name` | 实验大类，如 `vmi`、`vme` |
| `exp_task` | 实验子任务，如 `left_vs_rest`、`right_vs_rest` |
| `is_started` | 该任务是否已经实际开始 |
| `subject_count` | 该任务下已统计到的被试数量 |
| `trial_count` | 该任务累计记录的 trial 数 |
| `cumulative_accuracy_percent` | 该任务最后一个 trial 时刻的累计准确率百分比 |
| `cumulative_accuracy_std_percent` | 该任务最后一个 trial 时刻的累计准确率历史标准差 |
| `accuracy_score` | 该任务准确率得分 |
| `avg_reaction_time_ms` | 该任务最后一个 trial 时刻的累计平均反应时间，单位毫秒 |
| `reaction_time_score` | 该任务反应时间得分 |
| `static_component_score` | 静态分，等于校准分 + 通道分 + 可选的模型大小分 |
| `dynamic_component_score` | 动态分，等于准确率得分 + 反应时间得分 |
| `channel_score` | 该任务通道得分 |
| `calibration_score` | 该任务校准得分 |
| `model_size_score` | 该任务模型大小得分，基于全队统一的 `model_artifacts` 目录大小计算；若未统计到则为 0 且不纳入 task_score |
| `task_score` | 该任务最终分，即最后一个 trial 的累计总分 |
| `baseline_score` | 该任务对应的 baseline 分 |
| `adjusted_task_score` | 经过 baseline 裁剪后的任务得分 |

### 10.3 subject_task_summary.csv

这是被试-任务级汇总表，每行表示某个被试在某个任务上的统计结果。

| 字段 | 中文解释 |
| --- | --- |
| `team_id` | 队伍标识 |
| `task_name` | 任务标识 |
| `subject_id` | 被试标识 |
| `accuracy_percent` | 该被试在该任务上的准确率百分比 |
| `trial_count` | 该被试在该任务上的 trial 数 |

### 10.4 {task_id}_score.csv

这是任务逐 trial 结果表，每行表示该任务推进到当前 trial 时的累计状态。

| 字段 | 中文解释 |
| --- | --- |
| `subject_id` | 当前 trial 对应的被试标识 |
| `session_id` | 当前 trial 对应的 session 标识 |
| `trial_id` | 当前 trial 序号 |
| `calibration_rounds` | 当前算法声明的每类校准轮数 |
| `calibration_score` | 校准项得分 |
| `channel_rounds` | 当前算法声明的通道数 |
| `channel_score` | 通道项得分 |
| `model_size_mb` | 当前赛队由平台直接统计得到的 `model_artifacts` 目录大小，单位 MB；若未统计到则为空 |
| `model_size_score` | 模型大小项得分；若平台未统计到目录大小则为 0 且不纳入累计总分 |
| `current_reaction_time_ms` | 当前 trial 的反应时间，单位毫秒 |
| `cumulative_avg_reaction_time_ms` | 截止当前 trial 的累计平均反应时间，单位毫秒 |
| `cumulative_avg_reaction_time_score` | 截止当前 trial 的累计平均反应时间得分 |
| `true_label` | 当前 trial 真实标签 |
| `predict_label` | 当前 trial 预测标签 |
| `cumulative_accuracy_percent` | 截止当前 trial 的累计准确率百分比 |
| `cumulative_accuracy_std_percent` | 截止当前 trial 的累计准确率历史标准差 |
| `cumulative_accuracy_score` | 截止当前 trial 的累计准确率得分 |
| `static_component_score` | 当前任务静态分合计 |
| `dynamic_component_score` | 当前任务动态分合计 |
| `cumulative_score` | 截止当前 trial 的累计总分 |
| `is_timeout` | 当前 trial 是否为超时记录 |

## 11. 需要特别注意的实现细节

### 11.1 `predict_label` 为 `None` 的情况

如果算法没有正确上报 `predict_label`，则：

- `predict_label` 可能为空
- `is_correct` 无法判定
- 对应 trial 计分会受到影响

因此算法端必须统一上报：

`predict_label`

### 11.2 未开始任务为什么在 `task_summary.csv` 中仍可能出现

因为任务顺序由：

`ChallengeMI.__resolve_configured_task_order()`

决定，它会综合：

- 已经真实运行过的任务
- baseline 配置中的任务

所以即使某任务尚未开跑，也可能在 `task_summary.csv` 中出现一行 0 值记录，并通过：

`is_started = False`

明确标识。

### 11.3 为什么总分可能被未开始任务拉低

当前总分是按任务列表整体平均，因此：

- 已开始任务有真实分数
- 未开始任务通常为 0

如果这些未开始任务仍在最终任务列表中，就会一起进入平均。

这属于当前总分设计的一部分，不是落盘错误。

