"""Pipeline runner that builds dbt models and generates pricing recommendations."""

import os
import shutil
import subprocess
import sys


def run_dbt():
    """Run dbt models (requires dbt to be installed in the current environment)."""

    project_dir = os.path.join(os.path.dirname(__file__), "..", "pricing_dbt")
    dbt_cmd = shutil.which("dbt")
    if not dbt_cmd:
        raise FileNotFoundError(
            "`dbt` executable not found in PATH. Make sure your virtualenv is activated."
        )

    cmd = [dbt_cmd, "run"]
    print("Running dbt in:", project_dir)
    subprocess.run(cmd, cwd=project_dir, check=True)


def run_pricing_model():
    """Run the pricing model pipeline."""

    script_path = os.path.join(os.path.dirname(__file__), "pricing_model.py")
    cmd = [sys.executable, script_path]
    print("Running pricing model:", script_path)
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    run_dbt()
    run_pricing_model()
