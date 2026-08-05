"""Pipeline runner: ingestion → dbt → pricing model."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys


def run_ingestion():
    script = os.path.join(os.path.dirname(__file__), "ingestion.py")
    print("Running ingestion:", script)
    subprocess.run([sys.executable, script], check=True)


def run_dbt():
    project_dir = os.path.join(os.path.dirname(__file__), "..", "pricing_dbt")
    dbt_cmd = shutil.which("dbt")
    if not dbt_cmd:
        raise FileNotFoundError(
            "`dbt` executable not found in PATH. Activate your virtualenv first."
        )

    profiles_dir = os.path.join(os.path.dirname(__file__), "..", ".dbt")
    os.makedirs(profiles_dir, exist_ok=True)
    profiles_path = os.path.join(profiles_dir, "profiles.yml")
    if not os.path.exists(profiles_path):
        with open(profiles_path, "w") as f:
            f.write(
                "pricing_dbt:\n"
                "  target: dev\n"
                "  outputs:\n"
                "    dev:\n"
                "      type: duckdb\n"
                "      path: ../data/processed/pricing_dbt.duckdb\n"
                "      threads: 1\n"
            )

    cmd = [dbt_cmd, "build", "--profiles-dir", profiles_dir]
    print("Running dbt in:", project_dir)
    subprocess.run(cmd, cwd=project_dir, check=True)


def run_pricing_model():
    script_path = os.path.join(os.path.dirname(__file__), "pricing_model.py")
    print("Running pricing model:", script_path)
    subprocess.run([sys.executable, script_path], check=True)


if __name__ == "__main__":
    run_ingestion()
    run_dbt()
    run_pricing_model()
