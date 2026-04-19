from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = ROOT / "dags"


@pytest.fixture(autouse=True)
def _airflow_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_SP_S3_BUCKET", "test-bucket")
    monkeypatch.setenv("AIRFLOW_VAR_KAGGLE_USERNAME", "test-user")
    monkeypatch.setenv("AIRFLOW_VAR_KAGGLE_KEY", "test-key")


def test_all_dags_import_without_errors() -> None:
    pytest.importorskip("airflow")
    try:
        from airflow.models.dagbag import DagBag
    except ModuleNotFoundError:
        try:
            from airflow.models import DagBag
        except (ImportError, ModuleNotFoundError):
            pytest.skip("DagBag is not available in this Airflow installation")

    sys.path.insert(0, str(DAGS_DIR))
    try:
        dag_bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    finally:
        sys.path.remove(str(DAGS_DIR))
    assert dag_bag.import_errors == {}, f"DAG import errors: {dag_bag.import_errors}"

    expected_dags = {
        "text_daily_pipeline",
        "spotify_charts",
        "spotify_eps",
        "spotify_eps_backfill",
        "spotify_eps_union",
        "spotify_kaggle_upload",
        "spotify_kaggle_update",
    }
    assert expected_dags.issubset(set(dag_bag.dags))


def test_dags_do_not_use_legacy_schedule_interval() -> None:
    dag_files = DAGS_DIR.rglob("*_dag.py")
    for dag_file in dag_files:
        content = dag_file.read_text(encoding="utf-8")
        assert "schedule_interval=" not in content, (
            f"Legacy schedule_interval found in {dag_file}"
        )
