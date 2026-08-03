from __future__ import annotations

from pathlib import Path


def collect_log_file_paths(project_root: str | Path) -> list[Path]:
    root = Path(project_root)
    candidate_root_list = [
        root / "app" / "JudgeWeb",
        root / "app" / "ProcessHub",
        root / "results",
    ]
    collected_paths: list[Path] = []
    for candidate_root in candidate_root_list:
        if not candidate_root.exists():
            continue
        for log_path in candidate_root.rglob("*.log"):
            if log_path not in collected_paths:
                collected_paths.append(log_path)
    return sorted(collected_paths)


def collect_log_snippets(project_root: str | Path, max_lines: int = 20) -> dict[str, list[str]]:
    snippet_map: dict[str, list[str]] = {}
    for log_path in collect_log_file_paths(project_root):
        line_list = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        snippet_map[str(log_path)] = line_list[-max(0, int(max_lines)):]
    return snippet_map
