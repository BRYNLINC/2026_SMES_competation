import hashlib
import os
import random


DEFAULT_GLOBAL_SEED = 2026
STAGE_SEED_NAMESPACE = "SMES_FINAL"


def build_stage_seed(
    subject_id,
    exp_name,
    exp_task,
    session_id,
    *,
    global_seed: int = DEFAULT_GLOBAL_SEED,
) -> int:
    stage_identity = "|".join(
        str(value or "").strip()
        for value in (subject_id, exp_name, exp_task, session_id)
    )
    seed_source = f"{STAGE_SEED_NAMESPACE}|{int(global_seed)}|{stage_identity}"
    return int.from_bytes(
        hashlib.sha256(seed_source.encode("utf-8")).digest()[:4],
        "big",
    )


def seed_everything(seed: int = DEFAULT_GLOBAL_SEED) -> int:
    normalized_seed = int(seed)
    os.environ["PYTHONHASHSEED"] = str(normalized_seed)
    os.environ.setdefault("CUBLAS_WORKSPACE_CONFIG", ":4096:8")

    random.seed(normalized_seed)

    try:
        import numpy as np

        np.random.seed(normalized_seed)
    except (ImportError, OSError):
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
    except (ImportError, OSError):
        pass

    return normalized_seed


def seed_everything_for_stage(
    subject_id,
    exp_name,
    exp_task,
    session_id,
    *,
    global_seed: int = DEFAULT_GLOBAL_SEED,
) -> int:
    stage_seed = build_stage_seed(
        subject_id,
        exp_name,
        exp_task,
        session_id,
        global_seed=global_seed,
    )
    seed_everything(stage_seed)
    return stage_seed
