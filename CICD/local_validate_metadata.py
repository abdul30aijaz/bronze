#!/usr/bin/env python3
"""
Standalone validator for MDIF metadata JSON files, mirroring the rules in
CICD/validate_metadata.py (which requires pyspark/dbutils and only runs inside
Databricks). Used as a pre-PR CI gate so a malformed auto-generated config
never reaches review as an open pull request.

Keep the rule set in this file in sync with CICD/validate_metadata.py by hand
-- there is no shared import because the Databricks notebook depends on
pyspark and dbutils, neither of which exist on a GitHub Actions runner.

Usage:
    python CICD/local_validate_metadata.py metadata/configs/ams_na_us_daily/ad_campaign_performance.json [...]
Exits non-zero if any file fails validation.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REQUIRED_FIELDS = ["source_name", "region", "market", "domain", "ingestion_steps"]
LAYER_REQUIRED_FIELDS = {
    "raw": ["schema_config"],
    "raw_trusted": ["load_type"],
}
VALID_FORMATS = {"excel", "csv", "parquet", "json", "delta"}
FORMATS_REQUIRING_OPTIONS = {"excel", "csv", "json"}
VALID_TRIGGER_TYPES = {"schedule", "file_arrival"}


def _is_api_landing(landing_cfg: dict) -> bool:
    return "parameters" in landing_cfg


def _validate_top_level(data: dict, errors: list) -> None:
    for field_ in REQUIRED_FIELDS:
        if field_ not in data or data[field_] is None:
            errors.append(f"Missing required field: '{field_}'")
    if "is_active" in data and not isinstance(data["is_active"], bool):
        errors.append(f"'is_active' must be boolean, got {type(data['is_active']).__name__}")


def _validate_landing(landing_cfg: dict, errors: list) -> None:
    if _is_api_landing(landing_cfg):
        parameters = landing_cfg.get("parameters")
        if not isinstance(parameters, dict):
            errors.append("'parameters' must be a dict (api source)")
        elif not parameters.get("notebook_path"):
            errors.append("Missing 'notebook_path' in landing.parameters")
    else:
        for field_ in ["source_path", "file_format"]:
            if not landing_cfg.get(field_):
                errors.append(f"Missing '{field_}' in landing")

        file_format = (landing_cfg.get("file_format") or "").lower()
        if file_format and file_format not in VALID_FORMATS:
            errors.append(f"Invalid file_format '{file_format}'")

        if file_format in FORMATS_REQUIRING_OPTIONS:
            opts_key = next((k for k in landing_cfg if k.endswith("_options")), None)
            if not opts_key:
                errors.append(f"Missing '{file_format}_options' block for format '{file_format}'")
            elif not isinstance(landing_cfg[opts_key], dict):
                errors.append(f"'{opts_key}' must be a dict")

        pii_columns = landing_cfg.get("pii_columns")
        if pii_columns is None:
            errors.append("'pii_columns' is required (use empty list if none)")
        elif not isinstance(pii_columns, list):
            errors.append("'pii_columns' must be a list")
        elif len(pii_columns) != len(set(pii_columns)):
            errors.append("'pii_columns' contains duplicates")


def _validate_raw(raw_cfg: dict, errors: list) -> None:
    for field_ in LAYER_REQUIRED_FIELDS["raw"]:
        if not raw_cfg.get(field_):
            errors.append(f"Missing '{field_}' in 'raw'")
    dedup = raw_cfg.get("deduplication")
    if dedup is not None and not isinstance(dedup, bool):
        errors.append(f"'deduplication' must be boolean, got {type(dedup).__name__}")


def _validate_raw_trusted(rt_cfg: dict, errors: list) -> None:
    for field_ in LAYER_REQUIRED_FIELDS["raw_trusted"]:
        if not rt_cfg.get(field_):
            errors.append(f"Missing '{field_}' in 'raw_trusted'")
    schema_evo = rt_cfg.get("schema_evolution")
    if schema_evo is not None and not isinstance(schema_evo, bool):
        errors.append(f"'schema_evolution' must be boolean, got {type(schema_evo).__name__}")


def _validate_notify(notify_cfg: dict, errors: list) -> None:
    if not isinstance(notify_cfg, dict):
        errors.append(f"'notify' must be a dict, got {type(notify_cfg).__name__}")
        return
    emails = notify_cfg.get("emails")
    if emails is None:
        errors.append("'notify.emails' is required")
    elif not isinstance(emails, list):
        errors.append("'notify.emails' must be a list")
    elif not emails:
        errors.append("'notify.emails' cannot be empty")
    elif not all(isinstance(e, str) for e in emails):
        errors.append("'notify.emails' must contain only strings")


def _validate_trigger(trigger_cfg: dict, errors: list) -> None:
    if not isinstance(trigger_cfg, dict):
        errors.append(f"'trigger' must be a dict, got {type(trigger_cfg).__name__}")
        return
    trigger_type = trigger_cfg.get("type")
    if not trigger_type:
        return
    if trigger_type not in VALID_TRIGGER_TYPES:
        errors.append(f"Invalid trigger.type '{trigger_type}'")
        return
    if trigger_type == "schedule":
        if not trigger_cfg.get("cron"):
            errors.append("'trigger.cron' required for schedule trigger")
        if not trigger_cfg.get("timezone"):
            errors.append("'trigger.timezone' required for schedule trigger")


def _validate_schema(data: dict, errors: list) -> None:
    for i, col in enumerate(data.get("schema", [])):
        if "column_name" not in col or "data_type" not in col:
            errors.append(f"Schema entry {i} missing required fields")


def validate_entry(data: dict) -> list:
    errors: list = []
    _validate_top_level(data, errors)

    ingestion_steps = data.get("ingestion_steps")
    if not ingestion_steps:
        errors.append("Missing 'ingestion_steps'")
        return errors

    landing_cfg = ingestion_steps.get("landing")
    if landing_cfg:
        _validate_landing(landing_cfg, errors)

    raw_cfg = ingestion_steps.get("raw")
    if raw_cfg:
        _validate_raw(raw_cfg, errors)

    rt_cfg = ingestion_steps.get("raw_trusted")
    if rt_cfg:
        _validate_raw_trusted(rt_cfg, errors)

    if data.get("notify") is not None:
        _validate_notify(data["notify"], errors)

    if data.get("trigger") is not None:
        _validate_trigger(data["trigger"], errors)

    _validate_schema(data, errors)
    return errors


def main(argv=None) -> int:
    files = argv if argv is not None else sys.argv[1:]
    if not files:
        print("Usage: local_validate_metadata.py <file.json> [...]", file=sys.stderr)
        return 2

    all_errors = {}
    for f in files:
        path = Path(f)
        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError as e:
            all_errors[f] = [f"Invalid JSON: {e}"]
            continue
        errors = validate_entry(data)
        if errors:
            all_errors[f] = errors
            print(f"FAIL {f}")
            for err in errors:
                print(f"  {err}")
        else:
            print(f"ok   {f}")

    if all_errors:
        print(f"\nValidation failed for {len(all_errors)} file(s)", file=sys.stderr)
        return 1
    print(f"\nValidation complete | {len(files)} file(s) passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
