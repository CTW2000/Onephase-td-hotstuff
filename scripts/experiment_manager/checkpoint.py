"""Pipeline checkpoint — JSON-backed save/load/resume state."""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from .models import CheckpointEntry, RunResult, RunStatus


class PipelineCheckpoint:
    def __init__(
        self,
        pipeline_id: str,
        runner_mode: str,
        entries: Optional[dict] = None,
        baselines: Optional[dict] = None,
        created_at: Optional[str] = None,
    ):
        self.pipeline_id = pipeline_id
        self.runner_mode = runner_mode
        self.entries: dict[str, CheckpointEntry] = entries or {}
        self.baselines: dict[str, dict] = baselines or {}
        self.created_at = created_at or datetime.now().isoformat()
        self.updated_at = self.created_at

    def is_completed(self, run_id: str) -> bool:
        entry = self.entries.get(run_id)
        return entry is not None and entry.status == RunStatus.COMPLETED

    def mark_running(self, run_id: str, experiment_name: str) -> None:
        self.entries[run_id] = CheckpointEntry(
            run_id=run_id,
            experiment_name=experiment_name,
            status=RunStatus.RUNNING,
        )

    def mark_completed(self, run_id: str, result: RunResult) -> None:
        if run_id in self.entries:
            self.entries[run_id].status = RunStatus.COMPLETED
            self.entries[run_id].result = result
            self.entries[run_id].error_message = None

    def mark_failed(self, run_id: str, error: str) -> None:
        if run_id in self.entries:
            self.entries[run_id].status = RunStatus.FAILED
            self.entries[run_id].error_message = error

    def reset_failed(self, experiment_name: Optional[str] = None) -> int:
        """Reset FAILED entries back to PENDING. Returns count of entries reset."""
        count = 0
        for entry in self.entries.values():
            if entry.status == RunStatus.FAILED:
                if experiment_name is None or entry.experiment_name == experiment_name:
                    entry.status = RunStatus.PENDING
                    entry.error_message = None
                    count += 1
        return count

    def save(self, path: str) -> None:
        self.updated_at = datetime.now().isoformat()
        data = {
            "pipeline_id": self.pipeline_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "runner_mode": self.runner_mode,
            "entries": {
                k: v.to_dict() for k, v in self.entries.items()
            },
            "baselines": self.baselines,
        }
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    @classmethod
    def load(cls, path: str) -> "PipelineCheckpoint":
        with open(path) as f:
            data = json.load(f)
        entries = {
            k: CheckpointEntry.from_dict(v)
            for k, v in data.get("entries", {}).items()
        }
        return cls(
            pipeline_id=data["pipeline_id"],
            runner_mode=data["runner_mode"],
            entries=entries,
            baselines=data.get("baselines", {}),
            created_at=data.get("created_at"),
        )

    def summary(self) -> dict:
        """Return a status summary: {status_name: count}."""
        counts = {}
        for entry in self.entries.values():
            counts[entry.status.value] = counts.get(entry.status.value, 0) + 1
        return counts

    def experiment_summary(self) -> dict:
        """Return per-experiment status breakdown."""
        by_experiment = {}
        for entry in self.entries.values():
            if entry.experiment_name not in by_experiment:
                by_experiment[entry.experiment_name] = {}
            s = entry.status.value
            by_experiment[entry.experiment_name][s] = (
                by_experiment[entry.experiment_name].get(s, 0) + 1
            )
        return by_experiment
