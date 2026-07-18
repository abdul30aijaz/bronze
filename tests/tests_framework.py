"""Project structure and Python syntax validation tests."""

import ast
import os
import re

import pytest


ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)


def get_all_py_files():
    """
    Collect all Python files in the project.
    Returns:
        list[str]: Absolute paths of all Python files.
    """
    py_files = []

    # Traverse the project directory and collect Python files.
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [
            d for d in dirnames
            if not d.startswith(".") and d != "__pycache__"
        ]

        for f in filenames:
            if f.endswith(".py"):
                py_files.append(os.path.join(dirpath, f))

    return py_files


def test_main_py_exists():
    """Verify the main entry script exists."""
    assert os.path.exists(os.path.join(ROOT, "main.py"))


def test_start_job_exists():
    """Verify the job starter script exists."""
    assert os.path.exists(os.path.join(ROOT, "start_job.py"))


def test_read_metadata_exists():
    """Verify the metadata reader exists."""
    assert os.path.exists(os.path.join(ROOT, "read_metadata.py"))


def test_requirements_exists():
    """Verify the requirements file exists."""
    assert os.path.exists(os.path.join(ROOT, "requirements.txt"))


def test_control_table_init_exists():
    """Verify the control table initialization script exists."""
    assert os.path.exists(
        os.path.join(ROOT, "CICD", "control_table_init.py")
    )


def test_connectors_folder_exists():
    """Verify the connectors directory exists."""
    assert os.path.isdir(os.path.join(ROOT, "connectors"))


def test_connector_registry_exists():
    """Verify the connector registry exists."""
    assert os.path.exists(
        os.path.join(ROOT, "connectors", "connector_registry.py")
    )


def test_transform_folder_exists():
    """Verify the transform directory exists."""
    assert os.path.isdir(os.path.join(ROOT, "transform"))


def test_utils_folder_exists():
    """Verify the utils directory exists."""
    assert os.path.isdir(os.path.join(ROOT, "utils"))


def test_logging_utils_exists():
    """Verify the logging utilities exist."""
    assert os.path.exists(os.path.join(ROOT, "utils", "logging_utils.py"))


def test_path_utils_exists():
    """Verify the path utilities exist."""
    assert os.path.exists(os.path.join(ROOT, "utils", "path_utils.py"))


def test_schema_config_exists():
    """Verify the schema configuration exists."""
    assert os.path.exists(os.path.join(ROOT, "utils", "schema_config.py"))


def test_validations_folder_exists():
    """Verify the validation directory exists."""
    assert os.path.isdir(os.path.join(ROOT, "validation"))


def test_requirements_has_great_expectations():
    """Verify great-expectations is listed in requirements."""
    with open(os.path.join(ROOT, "requirements.txt")) as f:
        content = f.read()
    assert "great-expectations" in content


def test_requirements_has_tenacity():
    """Verify tenacity is listed in requirements."""
    with open(os.path.join(ROOT, "requirements.txt")) as f:
        content = f.read()
    assert "tenacity" in content


def test_requirements_has_openpyxl():
    """Verify openpyxl is listed in requirements."""
    with open(os.path.join(ROOT, "requirements.txt")) as f:
        content = f.read()
    assert "openpyxl" in content


def test_all_python_files_have_valid_syntax():
    """
    Validate the syntax of all Python files in the project.
    Returns:
        None
    """
    py_files = get_all_py_files()
    assert py_files, "No Python files found"

    for filepath in py_files:
        with open(filepath, encoding="utf-8") as f:
            source = f.read()

        # Remove Databricks notebook magic commands before parsing.
        cleaned_source = re.sub(r"^\s*%.*$", "", source, flags=re.MULTILINE)

        try:
            ast.parse(cleaned_source)
        except SyntaxError as e:
            pytest.fail(f"Syntax error in {filepath}:\n{e}")


def test_python_files_are_not_empty():
    """
    Verify all Python files contain executable code.
    Returns:
        None
    """
    py_files = get_all_py_files()

    for filepath in py_files:
        with open(filepath, encoding="utf-8") as f:
            lines = f.readlines()

        # Exclude comments, blank lines, and standalone docstrings.
        code_lines = [
            line for line in lines
            if line.strip()
            and not line.strip().startswith("#")
            and not line.strip().startswith('"""')
            and not line.strip().startswith("'''")
        ]

        rel_path = os.path.relpath(filepath, ROOT)
        assert code_lines, f"{rel_path}: file is empty or fully commented out"