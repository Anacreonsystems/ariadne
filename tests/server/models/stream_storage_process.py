"""Crash-boundary worker exercising only the actual native storage implementation."""

import asyncio
import json
import os
import sys

from prefect.server.database.dependencies import provide_database_interface
from prefect.server.models.streams import NativeStreamStore
from prefect.server.schemas.streams import ConsumerBinding, DeliveryToken
from prefect.settings import PREFECT_SERVER_DATABASE_CONNECTION_URL, temporary_settings


async def run(command: dict) -> None:
    store = NativeStreamStore(provide_database_interface())
    binding = ConsumerBinding.model_validate(command["binding"])
    if command["operation"] == "read":
        delivery = await store.read(binding)
        print(delivery.model_dump_json(), flush=True)
        os._exit(73)
    token = DeliveryToken.model_validate(command["token"])
    result = await store.complete(
        binding, token, "YIELD", {"signed": 200}, {"count": 1}
    )
    print(result.model_dump_json(), flush=True)
    os._exit(74)


if __name__ == "__main__":
    command = json.loads(sys.stdin.read())
    with temporary_settings(
        {PREFECT_SERVER_DATABASE_CONNECTION_URL: command["connection_url"]}
    ):
        asyncio.run(run(command))
