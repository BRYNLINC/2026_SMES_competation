from __future__ import annotations

import csv
import json
import os
import pytest
import shutil
import subprocess
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from tests.helpers.pytest_run_lock import ConcurrentPytestRunError, PytestRunLock


REPORT_FIELDNAME_LIST = [
    "run_id",
    "started_at",
    "finished_at",
    "duration_seconds",
    "test_id",
    "pytest_nodeid",
    "layer",
    "category",
    "priority",
    "requirement",
    "tested_file",
    "tested_function",
    "scenario",
    "input_summary",
    "fault_injection",
    "expected_result",
    "actual_result",
    "status",
    "failure_type",
    "failure_message",
    "artifact_dir",
    "log_files",
    "result_files",
    "screenshot_files",
    "runtime_state_db",
    "environment",
    "host_role",
    "team_count",
    "algorithm_profiles",
    "network_profile",
    "recovery_mode",
    "git_commit",
    "git_dirty",
    "notes",
]


def windows_extended_length_path(path: Path, *, platform_name: str | None = None) -> str:
    resolved_path = str(Path(path).resolve())
    if (platform_name or os.name) != "nt" or resolved_path.startswith("\\\\?\\"):
        return resolved_path
    if resolved_path.startswith("\\\\"):
        return "\\\\?\\UNC\\" + resolved_path[2:]
    return "\\\\?\\" + resolved_path

SUMMARY_FIELDNAME_LIST = [
    "run_id",
    "generated_at",
    "group_by",
    "group_value",
    "total_count",
    "passed_count",
    "failed_count",
    "skipped_count",
    "xfailed_count",
    "error_count",
    "pass_rate_percent",
    "total_duration_seconds",
    "blocking_failure_count",
    "release_gate",
    "blocking_failure_test_ids",
]
JSON_REPLACE_RETRY_COUNT = 8
JSON_REPLACE_RETRY_DELAY_SECONDS = 0.1


@dataclass
class TestReportRow:
    __test__ = False
    run_id: str
    started_at: str
    finished_at: str
    duration_seconds: str
    test_id: str
    pytest_nodeid: str
    layer: str
    category: str
    priority: str
    requirement: str
    tested_file: str
    tested_function: str
    scenario: str
    input_summary: str
    fault_injection: str
    expected_result: str
    actual_result: str
    status: str
    failure_type: str
    failure_message: str
    artifact_dir: str
    log_files: str
    result_files: str
    screenshot_files: str
    runtime_state_db: str
    environment: str
    host_role: str
    team_count: str
    algorithm_profiles: str
    network_profile: str
    recovery_mode: str
    git_commit: str
    git_dirty: str
    notes: str


class CsvReportWriter:
    def write_report(self, row_list: list[TestReportRow], path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with _open_csv_for_write(path) as file:
            writer = csv.DictWriter(file, fieldnames=REPORT_FIELDNAME_LIST)
            writer.writeheader()
            for row in row_list:
                writer.writerow({key: sanitize_csv_cell(value) for key, value in asdict(row).items()})

    def write_summary(self, row_list: list[dict[str, Any]], path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with _open_csv_for_write(path) as file:
            writer = csv.DictWriter(file, fieldnames=SUMMARY_FIELDNAME_LIST)
            writer.writeheader()
            for row in row_list:
                writer.writerow({key: sanitize_csv_cell(row.get(key, "")) for key in SUMMARY_FIELDNAME_LIST})


def sanitize_csv_cell(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\r", " ").replace("\n", "\\n")
    if text[:1] in {"=", "+", "-", "@"}:
        return "'" + text
    return text


class CsvReportPlugin:
    def __init__(self, project_root: Path):
        self.project_root = Path(project_root)
        self.run_id = time.strftime("%Y%m%d-%H%M%S") + "-local"
        self.run_lock = PytestRunLock(
            artifacts_root=self.project_root / "tests" / "artifacts",
            run_id=self.run_id,
        )
        self.latest_root = self.project_root / "tests" / "artifacts" / "latest"
        self.artifact_root = self.latest_root
        self.failed_root = self.artifact_root / "failed_test_artifacts"
        self.run_manifest_path = self.latest_root / "run_manifest.json"
        self.live_status_path = self.latest_root / "live_status.json"
        self.writer = CsvReportWriter()
        self.row_list: list[TestReportRow] = []
        self.started_at_by_nodeid: dict[str, float] = {}
        self.metadata_missing_test_id_list: list[str] = []
        self.git_commit, self.git_dirty = self._resolve_git_state()

    def pytest_sessionstart(self, session) -> None:
        try:
            self.run_lock.acquire()
        except ConcurrentPytestRunError as exc:
            raise pytest.UsageError(str(exc)) from exc
        try:
            self._reset_latest_artifact_root()
            self._prune_non_latest_artifacts()
            self.artifact_root.mkdir(parents=True, exist_ok=True)
            self.failed_root.mkdir(parents=True, exist_ok=True)
            self._write_json_file(
                self.run_manifest_path,
                {
                    "run_id": self.run_id,
                    "status": "running",
                    "started_at": _to_iso(_current_epoch_time()),
                    "project_root": str(self.project_root),
                    "artifact_root": str(self.artifact_root),
                },
            )
            self._write_live_status(phase="sessionstart", status="running")
        except BaseException:
            self.run_lock.release()
            raise

    def pytest_runtest_logstart(self, nodeid: str, location) -> None:
        self.started_at_by_nodeid[nodeid] = time.time()
        self._write_live_status(phase="runtest_logstart", status="running", current_test=nodeid)

    def pytest_runtest_logreport(self, report) -> None:
        if report.outcome != "skipped" or report.when not in {"setup", "call"}:
            return
        reason = self._format_skip_reason(report)
        print(f"[skip] {report.nodeid} :: {reason}", flush=True)
        self._write_live_status(
            phase="skip",
            status="running",
            current_test=report.nodeid,
            extra={"skip_reason": reason},
        )

    def pytest_runtest_makereport(self, item, call):
        if call.when not in {"setup", "call"}:
            return
        if call.when == "setup" and call.excinfo is None:
            return
        if call.when == "call" and item.nodeid in {row.pytest_nodeid for row in self.row_list}:
            return
        started_at_epoch = self.started_at_by_nodeid.get(item.nodeid, call.start)
        finished_at_epoch = call.stop
        metadata = self._extract_metadata(item)
        status = "passed"
        failure_type = ""
        failure_message = ""
        artifact_dir = self.artifact_root / metadata["test_id"]
        if call.excinfo is not None:
            failure_message = str(call.excinfo.value)
            if call.excinfo.errisinstance(pytest.skip.Exception):  # type: ignore[name-defined]
                status = "skipped"
                failure_type = "skip"
                artifact_dir = self.artifact_root / metadata["test_id"]
            else:
                artifact_dir = self.failed_root / metadata["test_id"]
                artifact_dir.mkdir(parents=True, exist_ok=True)
                traceback_path = artifact_dir / "traceback.txt"
                traceback_path.write_text(str(call.excinfo), encoding="utf-8")
                status = "failed"
                failure_type = "assertion"
                if call.excinfo.errisinstance(Exception) and call.excinfo.typename not in {"AssertionError"}:
                    failure_type = "error"
        row = TestReportRow(
            run_id=self.run_id,
            started_at=_to_iso(started_at_epoch),
            finished_at=_to_iso(finished_at_epoch),
            duration_seconds=f"{max(0.0, finished_at_epoch - started_at_epoch):.3f}",
            test_id=metadata["test_id"],
            pytest_nodeid=item.nodeid,
            layer=metadata["layer"],
            category=metadata["category"],
            priority=metadata["priority"],
            requirement=metadata["requirement"],
            tested_file=metadata["tested_file"],
            tested_function=metadata["tested_function"],
            scenario=metadata["scenario"],
            input_summary="",
            fault_injection="none",
            expected_result=metadata["expected_result"],
            actual_result=status if status == "passed" else failure_message,
            status=status,
            failure_type=failure_type,
            failure_message=failure_message,
            artifact_dir=str(artifact_dir),
            log_files="",
            result_files="",
            screenshot_files="",
            runtime_state_db="",
            environment=os.environ.get("BCI_TEST_ENVIRONMENT", "local"),
            host_role=os.environ.get("BCI_TEST_HOST_ROLE", "single_machine_simulation"),
            team_count=os.environ.get("BCI_TEST_TEAM_COUNT", ""),
            algorithm_profiles=os.environ.get("BCI_TEST_ALGORITHM_PROFILES", ""),
            network_profile=os.environ.get("BCI_TEST_NETWORK_PROFILE", "normal"),
            recovery_mode=os.environ.get("BCI_TEST_RECOVERY_MODE", "none"),
            git_commit=self.git_commit,
            git_dirty=str(self.git_dirty).lower(),
            notes=metadata["notes"],
        )
        self.row_list.append(row)
        self._write_live_status(
            phase="runtest_makereport",
            status="running",
            current_test=item.nodeid,
            extra={
                "last_recorded_test_id": metadata["test_id"],
                "last_recorded_status": status,
                "recorded_count": len(self.row_list),
            },
        )

    def pytest_sessionfinish(self, session, exitstatus: int) -> None:
        try:
            report_path = self.artifact_root / "test_report.csv"
            summary_path = self.artifact_root / "test_summary.csv"
            self.writer.write_report(self.row_list, report_path)
            summary_row_list = self._build_summary_rows()
            self._write_summary_with_fallback(summary_row_list, summary_path)
            self._copy_latest(report_path, summary_path)
            self._write_json_file(
                self.run_manifest_path,
                {
                    "run_id": self.run_id,
                    "status": "finished",
                    "started_at": self._read_existing_started_at(),
                    "finished_at": _to_iso(_current_epoch_time()),
                    "exitstatus": int(exitstatus),
                    "row_count": len(self.row_list),
                    "project_root": str(self.project_root),
                    "artifact_root": str(self.artifact_root),
                },
            )
            self._write_live_status(
                phase="sessionfinish",
                status="finished",
                extra={"exitstatus": int(exitstatus), "recorded_count": len(self.row_list)},
            )
        finally:
            self.run_lock.release()

    def pytest_unconfigure(self, config) -> None:
        self.run_lock.release()

    def _build_summary_rows(self) -> list[dict[str, Any]]:
        row_list: list[dict[str, Any]] = []
        for group_by in ("layer", "category", "priority"):
            group_value_dict: dict[str, list[TestReportRow]] = {}
            for row in self.row_list:
                group_value = getattr(row, group_by) or "unknown"
                group_value_dict.setdefault(group_value, []).append(row)
            for group_value, grouped_row_list in sorted(group_value_dict.items()):
                total_count = len(grouped_row_list)
                passed_count = sum(1 for row in grouped_row_list if row.status == "passed")
                failed_count = sum(1 for row in grouped_row_list if row.status == "failed")
                skipped_count = sum(1 for row in grouped_row_list if row.status == "skipped")
                xfailed_count = sum(1 for row in grouped_row_list if row.status == "xfailed")
                error_count = sum(1 for row in grouped_row_list if row.status == "error")
                blocking_failure_id_list = [
                    row.test_id
                    for row in grouped_row_list
                    if row.priority == "P0" and row.status in {"failed", "error"}
                ]
                total_duration = sum(float(row.duration_seconds or 0.0) for row in grouped_row_list)
                row_list.append(
                    {
                        "run_id": self.run_id,
                        "generated_at": _to_iso(_current_epoch_time()),
                        "group_by": group_by,
                        "group_value": group_value,
                        "total_count": total_count,
                        "passed_count": passed_count,
                        "failed_count": failed_count,
                        "skipped_count": skipped_count,
                        "xfailed_count": xfailed_count,
                        "error_count": error_count,
                        "pass_rate_percent": f"{(passed_count / total_count * 100.0) if total_count else 0.0:.2f}",
                        "total_duration_seconds": f"{total_duration:.3f}",
                        "blocking_failure_count": len(blocking_failure_id_list),
                        "release_gate": "blocked" if blocking_failure_id_list else "passed",
                        "blocking_failure_test_ids": "|".join(blocking_failure_id_list),
                    }
                )
        return row_list

    def _extract_metadata(self, item) -> dict[str, str]:
        metadata = {
            "test_id": self._marker_value(item, "test_id") or self._fallback_test_id(item.nodeid),
            "layer": self._marker_value(item, "layer") or "unknown",
            "category": self._marker_value(item, "category") or "unknown",
            "priority": self._marker_value(item, "priority") or "P2",
            "requirement": self._marker_value(item, "requirement") or "",
            "tested_file": "",
            "tested_function": "",
            "scenario": item.name,
            "expected_result": "",
            "notes": "",
        }
        tested_marker = item.get_closest_marker("tested")
        if tested_marker is not None:
            metadata["tested_file"] = str(tested_marker.kwargs.get("file", ""))
            metadata["tested_function"] = str(tested_marker.kwargs.get("function", ""))
        metadata["expected_result"] = self._marker_value(item, "expected_result") or ""
        missing_field_name_list = [
            field_name
            for field_name in ("test_id", "layer", "category", "priority", "tested_file", "tested_function")
            if not metadata.get(field_name)
        ]
        if missing_field_name_list:
            metadata["notes"] = "metadata_incomplete:" + ",".join(missing_field_name_list)
        return metadata

    @staticmethod
    def _format_skip_reason(report) -> str:
        longrepr = getattr(report, "longrepr", None)
        if isinstance(longrepr, tuple) and len(longrepr) >= 3:
            return str(longrepr[2]).strip()
        if hasattr(longrepr, "reprcrash") and getattr(longrepr, "reprcrash", None) is not None:
            message = getattr(longrepr.reprcrash, "message", "")
            if message:
                return str(message).strip()
        return str(longrepr or "skipped").strip()

    @staticmethod
    def _marker_value(item, name: str) -> str:
        marker = item.get_closest_marker(name)
        if marker is None:
            return ""
        if marker.args:
            return str(marker.args[0])
        return str(marker.kwargs.get("value", ""))

    @staticmethod
    def _fallback_test_id(nodeid: str) -> str:
        safe = (
            nodeid.replace("::", "__")
            .replace("/", "_")
            .replace("\\", "_")
            .replace("[", "_")
            .replace("]", "_")
        )
        return safe[:120]

    def _copy_latest(self, report_path: Path, summary_path: Path) -> None:
        if report_path.parent.resolve() == self.latest_root.resolve():
            return
        self.latest_root.mkdir(parents=True, exist_ok=True)
        shutil.copy2(report_path, self.latest_root / "test_report.csv")
        shutil.copy2(summary_path, self.latest_root / "test_summary.csv")

    def _write_summary_with_fallback(self, summary_row_list: list[dict[str, Any]], summary_path: Path) -> None:
        pointer_path = self.artifact_root / "test_summary.latest.txt"
        try:
            self.writer.write_summary(summary_row_list, summary_path)
            pointer_path.unlink(missing_ok=True)
        except PermissionError:
            for stale_tmp_path in self.artifact_root.glob("*.tmp"):
                stale_tmp_path.unlink(missing_ok=True)
            fallback_path = self.artifact_root / f"test_summary.{self.run_id}.csv"
            self.writer.write_summary(summary_row_list, fallback_path)
            pointer_path.write_text(fallback_path.name, encoding="utf-8")

    def _reset_latest_artifact_root(self) -> None:
        if self.latest_root.exists():
            try:
                shutil.rmtree(windows_extended_length_path(self.latest_root))
            except OSError as exc:
                raise RuntimeError(
                    "cannot reset pytest latest artifacts: "
                    f"path={self.latest_root} error={type(exc).__name__}: {exc}. "
                    "A stale test process may still be using this directory; stop it before rerunning."
                ) from exc
        if self.latest_root.exists():
            raise RuntimeError(
                "cannot reset pytest latest artifacts: "
                f"path still exists after removal: {self.latest_root}. "
                "A stale test process may still be using this directory; stop it before rerunning."
            )
        self.latest_root.mkdir(parents=True, exist_ok=True)

    def _write_live_status(
        self,
        phase: str,
        status: str,
        current_test: str = "",
        extra: dict[str, Any] | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "run_id": self.run_id,
            "phase": phase,
            "status": status,
            "updated_at": _to_iso(_current_epoch_time()),
            "current_test": current_test,
            "recorded_count": len(self.row_list),
        }
        if extra:
            payload.update(extra)
        self._write_json_file(self.live_status_path, payload)

    def _read_existing_started_at(self) -> str:
        try:
            if not self.run_manifest_path.exists():
                return ""
            payload = json.loads(self.run_manifest_path.read_text(encoding="utf-8"))
            return str(payload.get("started_at") or "")
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            return ""

    @staticmethod
    def _write_json_file(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(f".{path.name}.tmp")
        payload_text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        last_permission_error: PermissionError | None = None
        for attempt_index in range(JSON_REPLACE_RETRY_COUNT):
            temp_path.write_text(payload_text, encoding="utf-8")
            try:
                os.replace(temp_path, path)
                return
            except PermissionError as exc:
                last_permission_error = exc
                time.sleep(JSON_REPLACE_RETRY_DELAY_SECONDS * float(attempt_index + 1))
        fallback_path = path.with_name(f"{path.stem}.{int(time.time() * 1000)}.json")
        fallback_path.write_text(payload_text, encoding="utf-8")
        if last_permission_error is not None:
            raise last_permission_error

    def _prune_non_latest_artifacts(self) -> None:
        artifacts_root = self.project_root / "tests" / "artifacts"
        if not artifacts_root.exists():
            return
        preserved_artifact_name_set = {
            "latest",
            self.run_lock.lock_path.name,
            self.run_lock.metadata_path.name,
        }
        for artifact_path in artifacts_root.iterdir():
            if artifact_path.name in preserved_artifact_name_set:
                continue
            if artifact_path.name.startswith("pytest_tmp"):
                continue
            if artifact_path.is_dir():
                shutil.rmtree(artifact_path, ignore_errors=True)
            else:
                try:
                    artifact_path.unlink(missing_ok=True)
                except PermissionError:
                    pass
        try:
            (self.latest_root / "report_write_error.txt").unlink(missing_ok=True)
        except PermissionError:
            pass
        for stale_path in self.latest_root.glob("*.tmp"):
            try:
                stale_path.unlink(missing_ok=True)
            except PermissionError:
                pass

    def _resolve_git_state(self) -> tuple[str, bool]:
        try:
            commit = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=str(self.project_root),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="ignore",
                check=False,
            )
            status = subprocess.run(
                ["git", "status", "--short"],
                cwd=str(self.project_root),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="ignore",
                check=False,
            )
            commit_text = (commit.stdout or "").strip() or "unknown"
            dirty = bool((status.stdout or "").strip())
            return commit_text, dirty
        except OSError:
            return "unknown", True


def _to_iso(timestamp_value: float) -> str:
    return datetime.fromtimestamp(timestamp_value, tz=timezone.utc).astimezone().isoformat()


def _current_epoch_time() -> float:
    return datetime.now(timezone.utc).timestamp()


def _open_csv_for_write(path: Path, retries: int = 5):
    last_error: PermissionError | None = None
    for attempt in range(retries):
        temp_path = path.with_name(f".{path.name}.tmp")
        try:
            file_obj = temp_path.open("w", encoding="utf-8-sig", newline="")
            return _AtomicReplaceFile(file_obj, temp_path, path)
        except PermissionError as exc:
            last_error = exc
            if attempt >= retries - 1:
                raise
            time.sleep(0.2 * (attempt + 1))
    if last_error is not None:
        raise last_error
    temp_path = path.with_name(f".{path.name}.tmp")
    return _AtomicReplaceFile(temp_path.open("w", encoding="utf-8-sig", newline=""), temp_path, path)


class _AtomicReplaceFile:
    def __init__(self, file_obj, temp_path: Path, target_path: Path):
        self._file_obj = file_obj
        self._temp_path = temp_path
        self._target_path = target_path

    def __enter__(self):
        return self._file_obj

    def __exit__(self, exc_type, exc, tb):
        self._file_obj.close()
        if exc_type is not None:
            self._temp_path.unlink(missing_ok=True)
            return False
        self._target_path.unlink(missing_ok=True)
        try:
            os.replace(self._temp_path, self._target_path)
        except PermissionError:
            self._temp_path.unlink(missing_ok=True)
            raise
        return False
