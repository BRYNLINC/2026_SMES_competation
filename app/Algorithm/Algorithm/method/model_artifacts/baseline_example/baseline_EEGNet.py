import torch
from torch import nn


class EEGNet(nn.Module):
    """
    适用于单trial EEG二分类的简化EEGNet实现。
    输入张量形状约定为 [batch, 1, channel, time]。
    """

    def __init__(
        self,
        channel_number: int,
        sample_number: int,
        class_number: int = 2,
        f1: int = 8,
        d: int = 2,
        f2: int = 16,
        kernel_length: int = 64,
        separable_kernel_length: int = 16,
        dropout_probability: float = 0.5,
    ):
        super().__init__()
        self.channel_number = channel_number
        self.sample_number = sample_number
        self.class_number = class_number

        self.temporal_block = nn.Sequential(
            nn.Conv2d(
                in_channels=1,
                out_channels=f1,
                kernel_size=(1, kernel_length),
                padding=(0, kernel_length // 2),
                bias=False,
            ),
            nn.BatchNorm2d(f1),
        )

        self.spatial_block = nn.Sequential(
            nn.Conv2d(
                in_channels=f1,
                out_channels=f1 * d,
                kernel_size=(channel_number, 1),
                groups=f1,
                bias=False,
            ),
            nn.BatchNorm2d(f1 * d),
            nn.ELU(),
            nn.AvgPool2d(kernel_size=(1, 4)),
            nn.Dropout(p=dropout_probability),
        )

        self.separable_block = nn.Sequential(
            nn.Conv2d(
                in_channels=f1 * d,
                out_channels=f1 * d,
                kernel_size=(1, separable_kernel_length),
                padding=(0, separable_kernel_length // 2),
                groups=f1 * d,
                bias=False,
            ),
            nn.Conv2d(
                in_channels=f1 * d,
                out_channels=f2,
                kernel_size=(1, 1),
                bias=False,
            ),
            nn.BatchNorm2d(f2),
            nn.ELU(),
            nn.AvgPool2d(kernel_size=(1, 8)),
            nn.Dropout(p=dropout_probability),
        )

        self.classifier = nn.Linear(self._infer_feature_dim(), class_number)

    def _infer_feature_dim(self) -> int:
        with torch.no_grad():
            dummy_input = torch.zeros(1, 1, self.channel_number, self.sample_number)
            feature = self.forward_features(dummy_input)
        return feature.shape[1]

    def forward_features(self, x: torch.Tensor) -> torch.Tensor:
        x = self.temporal_block(x)
        x = self.spatial_block(x)
        x = self.separable_block(x)
        return torch.flatten(x, start_dim=1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        feature = self.forward_features(x)
        return self.classifier(feature)
