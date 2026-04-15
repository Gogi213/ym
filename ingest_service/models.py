from __future__ import annotations

from dataclasses import dataclass

from pydantic import BaseModel


class ResetPayload(BaseModel):
    action: str
    run_date: str


@dataclass(frozen=True)
class IngestSettings:
    ingest_token: str


@dataclass(frozen=True)
class PipelineRunStatus:
    run_date: str
