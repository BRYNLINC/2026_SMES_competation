# model_artifacts

该目录是比赛指定模型目录。

规则：

1. 所有参与在线推理的已学习参数、模板、权重、统计量，必须全部存放于该目录。
2. 目录外参数一律视为违规。
3. 选手端不可修改的算法框架层会在启动阶段统计该目录下已有普通文件总大小。
4. 框架统计结果会作为 `platform_model_size_mb` 上报给裁判机。

baseline 示例说明：

1. 当前 baseline 实际运行入口位于 `baseline_example/` 子目录。
2. 该子目录中包含实际运行所使用的 `AlgorithmImplement.py`、`baseline_EEGNet.py`、`baseline_preprocessing.py`。
3. 参赛选手主要应修改 `AlgorithmImplement.__init__()`、`AlgorithmImplement.calibrate()`、`AlgorithmImplement.predict()`。

