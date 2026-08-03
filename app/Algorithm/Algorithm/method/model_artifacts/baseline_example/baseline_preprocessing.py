from __future__ import annotations

import numpy as np
import torch
from scipy.signal import butter, filtfilt


class EEGNetPreprocessor:
    def __init__(self, eps: float = 1e-6):
        self._eps = eps
        self._bandpass_coefficients_by_sample_rate: dict[int, tuple[np.ndarray, np.ndarray]] = {}

    def preprocess_single_trial(
        self,
        trial_data: np.ndarray,
        channel_number: int,
        sample_rate: int,
        device: torch.device,
    ) -> torch.Tensor:
        eeg_data = self._extract_eeg_data(trial_data=trial_data, channel_number=channel_number)
        eeg_data = self._bandpass_filter_eeg_data(eeg_data=eeg_data, sample_rate=sample_rate)
        normalized_eeg_data = self._normalize_eeg_data(eeg_data)
        return torch.from_numpy(normalized_eeg_data[None, None, :, :]).to(device=device, dtype=torch.float32)

    def preprocess_trial_batch(self, trial_batch: np.ndarray, sample_rate: int, device: torch.device) -> torch.Tensor:
        trial_batch = np.asarray(trial_batch, dtype=np.float32)
        if trial_batch.ndim != 3:
            raise ValueError(f"trial_batch 维度必须为3，实际为 {trial_batch.ndim}")
        normalized_trial_batch = np.stack(
            [
                self._normalize_eeg_data(
                    self._bandpass_filter_eeg_data(single_trial, sample_rate=sample_rate)
                )
                for single_trial in trial_batch
            ],
            axis=0,
        )
        return torch.from_numpy(normalized_trial_batch[:, None, :, :]).to(device=device, dtype=torch.float32)

    def _extract_eeg_data(self, trial_data: np.ndarray, channel_number: int) -> np.ndarray:
        trial_data = np.asarray(trial_data, dtype=np.float32)
        if trial_data.ndim != 2:
            raise ValueError(f"trial_data 维度必须为2，实际为 {trial_data.ndim}")
        if trial_data.shape[0] == channel_number + 1:
            trial_data = trial_data[:channel_number, :]
        elif trial_data.shape[0] != channel_number:
            raise ValueError(
                f"trial_data 行数必须为 {channel_number} 或 {channel_number + 1}，"
                f"实际为 {trial_data.shape[0]}"
            )
        return trial_data.astype(np.float32, copy=False)

    def _normalize_eeg_data(self, eeg_data: np.ndarray) -> np.ndarray:
        eeg_data = np.asarray(eeg_data, dtype=np.float32)
        mean_value = eeg_data.mean(axis=1, keepdims=True)
        std_value = eeg_data.std(axis=1, keepdims=True)
        std_value = np.maximum(std_value, self._eps)
        return ((eeg_data - mean_value) / std_value).astype(np.float32, copy=False)

    def _bandpass_filter_eeg_data(self, eeg_data: np.ndarray, sample_rate: int) -> np.ndarray:
        eeg_data = np.asarray(eeg_data, dtype=np.float32)
        normalized_sample_rate = int(sample_rate)
        if normalized_sample_rate not in self._bandpass_coefficients_by_sample_rate:
            nyquist_frequency = 0.5 * float(normalized_sample_rate)
            self._bandpass_coefficients_by_sample_rate[normalized_sample_rate] = butter(
                N=4,
                Wn=[1.0 / nyquist_frequency, 40.0 / nyquist_frequency],
                btype='bandpass',
            )
        bandpass_b, bandpass_a = self._bandpass_coefficients_by_sample_rate[normalized_sample_rate]
        # EEGNet baseline 在标准化之前先做 1-40Hz 带通滤波，去掉明显的低频漂移和高频噪声。
        filtered_eeg_data = filtfilt(bandpass_b, bandpass_a, eeg_data, axis=1)
        return filtered_eeg_data.astype(np.float32, copy=False)
