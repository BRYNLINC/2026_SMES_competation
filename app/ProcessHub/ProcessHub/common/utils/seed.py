import os
import random


DEFAULT_GLOBAL_SEED = 2026


def seed_everything(seed: int = DEFAULT_GLOBAL_SEED) -> int:
    normalized_seed = int(seed)
    os.environ["PYTHONHASHSEED"] = str(normalized_seed)
    os.environ.setdefault("CUBLAS_WORKSPACE_CONFIG", ":4096:8")

    random.seed(normalized_seed)

    try:
        import numpy as np

        np.random.seed(normalized_seed)
    except ImportError:
        pass

    try:
        import torch

        torch.manual_seed(normalized_seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed(normalized_seed)
            torch.cuda.manual_seed_all(normalized_seed)
        if hasattr(torch.backends, "cudnn"):
            torch.backends.cudnn.benchmark = False
            torch.backends.cudnn.deterministic = True
        try:
            torch.use_deterministic_algorithms(True, warn_only=True)
        except Exception:
            pass
    except ImportError:
        pass

    return normalized_seed
