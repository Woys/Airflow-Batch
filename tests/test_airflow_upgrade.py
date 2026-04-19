from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]


def test_dockerfile_uses_airflow_3_1_8() -> None:
    dockerfile = ROOT / "Dockerfile"
    content = dockerfile.read_text(encoding="utf-8")

    pattern = r"^FROM\s+apache/airflow:3\.1\.8\s*$"
    assert re.search(pattern, content, flags=re.MULTILINE), (
        "Dockerfile must pin apache/airflow:3.1.8"
    )


def test_compose_references_airflow_3_1_8() -> None:
    compose_file = ROOT / "docker-compose.yaml"
    content = compose_file.read_text(encoding="utf-8")

    assert "apache/airflow:3.1.8" in content
