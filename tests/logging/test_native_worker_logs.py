"""A registered local worker must preserve the self-hosted log wire contract."""

import logging
from uuid import uuid4

import pytest

from prefect.logging.handlers import APILogHandler
from prefect.server.schemas.actions import LogCreate as ServerLogCreate
from prefect.settings import PREFECT_API_URL, PREFECT_CLOUD_API_URL, temporary_settings


@pytest.mark.parametrize("cloud", [False, True])
def test_run_log_with_worker_identity_uses_backend_log_capability(cloud: bool):
    record = logging.LogRecord(
        "prefect.flow_runs", 20, __file__, 1, "document", (), None
    )
    record.flow_run_id = uuid4()
    record.worker_id = uuid4()
    url = (
        PREFECT_CLOUD_API_URL.value() + "/accounts/test/workspaces/test"
        if cloud
        else "http://localhost:4200/api"
    )
    with temporary_settings({PREFECT_API_URL: url}):
        payload = APILogHandler().prepare(record)
    assert ("worker_id" in payload) is cloud
    assert payload["flow_run_id"] == str(record.flow_run_id)
    if not cloud:
        payload.pop("__payload_size__")
        ServerLogCreate.model_validate(payload)
