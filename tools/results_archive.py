from pathlib import Path

from tools import results_snapshot as _snapshot


ARCHIVE_DIR_NAME = _snapshot.ARCHIVE_DIR_NAME


def _build_archive_name(reason: str) -> str:
    return _snapshot.build_archive_name(reason)


def _iter_active_result_children(results_root: Path) -> list[Path]:
    return _snapshot.iter_active_children(results_root)


def _path_has_payload(path: Path) -> bool:
    return _snapshot._path_has_payload(path)


def has_active_results_payload(project_root: Path) -> bool:
    return _snapshot.has_active_payload(project_root)


def archive_results_snapshot(project_root: Path, reason: str) -> dict | None:
    return _snapshot.archive_snapshot(project_root, reason)


def verify_archive_manifest(archive_root: Path) -> dict:
    return _snapshot.verify_archive(archive_root)


def repair_finished_results(project_root: Path) -> dict:
    return _snapshot.repair_finished_results(project_root)


def clear_active_results_root(project_root: Path) -> None:
    _snapshot.clear_active(project_root)


def archive_and_clear_active_results(project_root: Path, reason: str) -> dict | None:
    return _snapshot.archive_and_clear(project_root, reason)
