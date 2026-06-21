from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field


class JobRunNotFound(RuntimeError):
    """Raised when no recorded job run can be located."""


JOB_RUNS_ROOT = Path(os.getenv("JOB_RUNS_LOCAL_DIR", "data/job_runs"))
DEFAULT_HISTORY_LIMIT = 14
MAX_HISTORY_LIMIT = 90
DETAIL_PREFIX = "[job-detail] "


class PricingIssue(BaseModel):
    source: str
    symbol: str
    issue_type: str
    details: Optional[str] = None


class JobRunSummary(BaseModel):
    job: str
    run_id: str
    status: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    positions_count: Optional[int] = None
    valuations_saved: bool = False
    s3_uploaded: bool = False
    warnings_count: int = 0
    errors_count: int = 0
    missing_prices_count: int = 0


class JobRunResponse(BaseModel):
    job: str
    run_id: str
    status: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    exit_code: Optional[int] = None
    positions_count: Optional[int] = None
    loaded: dict[str, int] = Field(default_factory=dict)
    pricing: dict[str, Any] = Field(default_factory=dict)
    counts_by_source: dict[str, dict[str, int]] = Field(default_factory=dict)
    pricing_issues: list[PricingIssue] = Field(default_factory=list)
    outputs: dict[str, Any] = Field(default_factory=dict)
    uploads: list[str] = Field(default_factory=list)
    s3_uploaded: bool = False
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    log_file: Optional[str] = None


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _slugify(label: str) -> str:
    label = label.strip().lower()
    replacements = {
        "santander manual holdings": "santander_holdings",
        "crypto manual holdings": "crypto_manual_holdings",
        "binance assets": "binance_assets",
        "ethereum assets": "ethereum_assets",
        "exodus ethereum assets": "exodus_eth_assets",
        "exodus bitcoin assets": "exodus_btc_assets",
        "metamask bitcoin assets": "metamask_btc_assets",
    }
    if label in replacements:
        return replacements[label]
    return re.sub(r"[^a-z0-9]+", "_", label).strip("_")


def _append_unique_issue(issues: list[dict[str, Any]], issue: dict[str, Any]) -> None:
    key = (issue.get("source"), issue.get("symbol"), issue.get("issue_type"))
    existing = {(i.get("source"), i.get("symbol"), i.get("issue_type")) for i in issues}
    if key not in existing:
        issues.append(issue)


def parse_job_run_log(log_text: str) -> dict[str, Any]:
    """Parse valuation stdout into dashboard-friendly structured details."""
    lines = [line.strip() for line in log_text.splitlines() if line.strip()]
    loaded: dict[str, int] = {}
    pricing: dict[str, Any] = {}
    outputs: dict[str, dict[str, str]] = {}
    uploads: list[str] = []
    warnings: list[str] = []
    errors: list[str] = []
    pricing_issues: list[dict[str, Any]] = []
    positions_count = None

    for line in lines:
        if line.startswith(DETAIL_PREFIX):
            try:
                detail = json.loads(line[len(DETAIL_PREFIX):])
            except json.JSONDecodeError:
                warnings.append(f"Could not parse job detail line: {line}")
                continue
            if isinstance(detail, dict):
                event = detail.get("event")
                source = str(detail.get("source") or "").lower()
                if event == "source_counts" and source:
                    source_counts = {k: v for k, v in detail.items() if isinstance(v, int)}
                    pricing.setdefault("by_source", {}).setdefault(source, {}).update(source_counts)
                elif event == "pricing_issue":
                    symbol = str(detail.get("symbol") or "").upper()
                    issue_type = str(detail.get("issue_type") or "unknown")
                    if source and symbol:
                        _append_unique_issue(
                            pricing_issues,
                            {
                                "source": source,
                                "symbol": symbol,
                                "issue_type": issue_type,
                                "details": detail.get("details"),
                            },
                        )
                elif event == "output_saved":
                    resource = str(detail.get("resource") or "")
                    file_type = str(detail.get("file_type") or "")
                    path = detail.get("path")
                    if resource and file_type and path:
                        outputs.setdefault(resource, {})[file_type] = str(path)
            continue

        loaded_match = re.search(r"Loaded\s+(\d+)\s+(.+?)\.?$", line)
        if loaded_match:
            loaded[_slugify(loaded_match.group(2))] = int(loaded_match.group(1))

        positions_match = re.search(r"Positions\s+\((\d+)\s+rows", line)
        if positions_match:
            positions_count = int(positions_match.group(1))

        binance_price_match = re.search(r"Priced\s+(\d+)\s+Binance assets\s+\(missing\s+(\d+)\)", line)
        if binance_price_match:
            pricing["binance_priced"] = int(binance_price_match.group(1))
            pricing["binance_missing"] = int(binance_price_match.group(2))
            pricing.setdefault("by_source", {}).setdefault("binance", {}).update(
                {"priced": int(binance_price_match.group(1)), "missing": int(binance_price_match.group(2))}
            )

        generic_price_match = re.search(r"Priced\s+(\d+)\s+(.+?) assets\.?$", line)
        if generic_price_match:
            source_key = _slugify(generic_price_match.group(2))
            pricing[f"{source_key}_priced"] = int(generic_price_match.group(1))

        missing_symbols_match = re.search(r"Missing\s+Binance prices(?:\s+for)?\s*:\s*(.+)$", line, flags=re.IGNORECASE)
        if missing_symbols_match:
            symbols = [s.strip().upper() for s in re.split(r"[,\s]+", missing_symbols_match.group(1)) if s.strip()]
            for symbol in symbols:
                _append_unique_issue(
                    pricing_issues,
                    {"source": "binance", "symbol": symbol, "issue_type": "missing_price", "details": "No Binance price found."},
                )

        saved_match = re.search(r"\[(positions|prices|fx|valuations)\]\s+(CSV|Parquet) saved ->\s+(.+)$", line)
        if saved_match:
            resource, file_type, path = saved_match.groups()
            outputs.setdefault(resource, {})[file_type.lower()] = path

        upload_match = re.search(r"\[ok\]\s+Uploaded ->\s+(.+)$", line)
        if upload_match:
            uploads.append(upload_match.group(1))

        lowered = line.lower()
        if "error" in lowered or "traceback" in lowered or "exception" in lowered:
            errors.append(line)
        elif "missing" in lowered or lowered.startswith("no "):
            warnings.append(line)

    counts_by_source: dict[str, dict[str, int]] = {}
    for key, count in loaded.items():
        source = key.removesuffix("_assets").removesuffix("_holdings")
        counts_by_source.setdefault(source, {})["loaded"] = count
    for source, counts in pricing.get("by_source", {}).items() if isinstance(pricing.get("by_source"), dict) else []:
        if isinstance(counts, dict):
            counts_by_source.setdefault(str(source), {}).update({str(k): int(v) for k, v in counts.items() if isinstance(v, int)})
    issue_counts_by_source: dict[str, int] = {}
    for issue in pricing_issues:
        issue_counts_by_source[issue["source"]] = issue_counts_by_source.get(issue["source"], 0) + 1
    for source, issue_count in issue_counts_by_source.items():
        source_counts = counts_by_source.setdefault(source, {})
        source_counts["missing"] = max(source_counts.get("missing", 0), issue_count)

    return {
        "positions_count": positions_count,
        "loaded": loaded,
        "pricing": pricing,
        "counts_by_source": counts_by_source,
        "pricing_issues": pricing_issues,
        "outputs": outputs,
        "uploads": uploads,
        "warnings": warnings,
        "errors": errors,
    }


def build_job_run_payload(
    *, job_name: str, run_id: str, started_at: str, finished_at: str, exit_code: int, log_file: Path
) -> dict[str, Any]:
    log_text = log_file.read_text(encoding="utf-8", errors="replace") if log_file.exists() else ""
    parsed = parse_job_run_log(log_text)
    try:
        duration_seconds = (_parse_ts(finished_at) - _parse_ts(started_at)).total_seconds()
    except Exception:
        duration_seconds = None
    outputs = parsed["outputs"]
    status = "success" if exit_code == 0 and "valuations" in outputs else "failed"
    return {
        "job": job_name,
        "run_id": run_id,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": duration_seconds,
        "exit_code": exit_code,
        **parsed,
        "s3_uploaded": bool(parsed["uploads"]),
        "log_file": str(log_file),
    }


def _job_dir(job_name: str) -> Path:
    sanitized = "".join(ch for ch in job_name if ch.isalnum() or ch in {"-", "_"}).strip()
    if not sanitized:
        raise JobRunNotFound("Invalid job name.")
    return JOB_RUNS_ROOT / sanitized


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except FileNotFoundError as exc:
        raise JobRunNotFound(f"No job run found at {path}.") from exc
    except json.JSONDecodeError as exc:
        raise JobRunNotFound(f"Job run file {path} is not valid JSON.") from exc
    if not isinstance(payload, dict):
        raise JobRunNotFound(f"Job run file {path} did not contain an object.")
    return payload


def _run_files(job_name: str) -> list[Path]:
    base_dir = _job_dir(job_name)
    if not base_dir.exists():
        return []
    return sorted(
        [path for path in base_dir.glob("*.json") if path.name != "latest.json"],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )


def _summarize(payload: dict[str, Any]) -> JobRunSummary:
    warnings = payload.get("warnings") if isinstance(payload.get("warnings"), list) else []
    errors = payload.get("errors") if isinstance(payload.get("errors"), list) else []
    pricing_issues = payload.get("pricing_issues") if isinstance(payload.get("pricing_issues"), list) else []
    return JobRunSummary(
        job=str(payload.get("job") or "unknown"),
        run_id=str(payload.get("run_id") or "unknown"),
        status=str(payload.get("status") or "unknown"),
        started_at=payload.get("started_at"),
        finished_at=payload.get("finished_at"),
        duration_seconds=payload.get("duration_seconds"),
        positions_count=payload.get("positions_count"),
        valuations_saved=bool(payload.get("outputs", {}).get("valuations")) if isinstance(payload.get("outputs"), dict) else False,
        s3_uploaded=bool(payload.get("s3_uploaded")),
        warnings_count=len(warnings),
        errors_count=len(errors),
        missing_prices_count=len(pricing_issues),
    )


def get_latest_job_run(job_name: str = "valuations") -> JobRunResponse:
    latest_path = _job_dir(job_name) / "latest.json"
    if latest_path.exists():
        return JobRunResponse(**_read_json(latest_path))

    files = _run_files(job_name)
    if not files:
        raise JobRunNotFound(f"No recorded runs for job '{job_name}'.")
    return JobRunResponse(**_read_json(files[0]))


def get_job_run_history(job_name: str = "valuations", limit: int = DEFAULT_HISTORY_LIMIT) -> list[JobRunSummary]:
    capped_limit = max(1, min(limit, MAX_HISTORY_LIMIT))
    files = _run_files(job_name)[:capped_limit]
    return [_summarize(_read_json(path)) for path in files]
