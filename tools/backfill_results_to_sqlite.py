import csv
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from tools.runtime_state_sqlite import (  # noqa: E402
    export_team_score_overview_csv,
    replace_team_subject_task_overview_rows,
    replace_team_task_overview_rows,
    replace_team_trial_record_rows,
    resolve_runtime_state_db_path,
    write_team_overview_row,
    write_team_score_overview_row,
)


RESULTS_ROOT = PROJECT_ROOT / 'results'
RUNTIME_STATE_DB_PATH = resolve_runtime_state_db_path(PROJECT_ROOT)


def read_csv_rows(file_path: Path) -> list[dict]:
    with file_path.open('r', encoding='utf-8-sig', newline='') as csv_file:
        return list(csv.DictReader(csv_file))


def read_csv_first_row(file_path: Path) -> dict | None:
    row_list = read_csv_rows(file_path)
    return row_list[0] if row_list else None


def iter_team_result_dirs() -> list[Path]:
    if not RESULTS_ROOT.exists():
        return []
    return sorted(
        item
        for item in RESULTS_ROOT.iterdir()
        if item.is_dir() and item.name not in {'live', 'control', 'history'}
    )


def backfill_team_dir(team_dir: Path) -> dict[str, int | str]:
    team_id = team_dir.name

    team_overview_row = read_csv_first_row(team_dir / '00_team_overview.csv')
    task_overview_row_list = read_csv_rows(team_dir / '01_task_overview.csv') if (team_dir / '01_task_overview.csv').exists() else []
    subject_task_overview_row_list = read_csv_rows(team_dir / '02_subject_task_overview.csv') if (team_dir / '02_subject_task_overview.csv').exists() else []
    trial_record_row_list = read_csv_rows(team_dir / '03_trial_records.csv') if (team_dir / '03_trial_records.csv').exists() else []

    if team_overview_row:
        resolved_team_id = str(team_overview_row.get('team_id') or team_id)
        team_overview_row['team_id'] = resolved_team_id
        write_team_overview_row(RUNTIME_STATE_DB_PATH, team_overview_row)
        write_team_score_overview_row(RUNTIME_STATE_DB_PATH, team_overview_row)
        team_id = resolved_team_id

    if task_overview_row_list:
        replace_team_task_overview_rows(RUNTIME_STATE_DB_PATH, team_id, task_overview_row_list)

    if subject_task_overview_row_list:
        replace_team_subject_task_overview_rows(RUNTIME_STATE_DB_PATH, team_id, subject_task_overview_row_list)

    if trial_record_row_list:
        replace_team_trial_record_rows(RUNTIME_STATE_DB_PATH, team_id, trial_record_row_list)

    return {
        'team_id': team_id,
        'task_count': len(task_overview_row_list),
        'subject_task_count': len(subject_task_overview_row_list),
        'trial_count': len(trial_record_row_list),
    }


def main() -> int:
    summary_list = [backfill_team_dir(team_dir) for team_dir in iter_team_result_dirs()]
    export_team_score_overview_csv(
        RUNTIME_STATE_DB_PATH,
        RESULTS_ROOT / '00_team_score_overview.csv',
        [
            'team_id',
            'total_score',
            'run_status',
            'updated_at',
            'observed_trial_count',
            'configured_task_count',
            'started_task_count',
            'mean_accuracy_percent',
            'avg_reaction_time_ms',
            'started_task_names',
        ],
    )

    print(f'backfilled_teams={len(summary_list)}')
    for item in summary_list:
        print(
            f"team_id={item['team_id']} task_count={item['task_count']} "
            f"subject_task_count={item['subject_task_count']} trial_count={item['trial_count']}"
        )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
