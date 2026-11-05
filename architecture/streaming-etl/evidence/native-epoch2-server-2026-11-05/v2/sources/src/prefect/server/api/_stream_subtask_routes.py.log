"""Pure exact HTTP route allowlist for least-privilege physical children."""

import re

_UUID = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
_DATA_POST = frozenset(
    "/streams/v1/" + path
    for path in (
        "sources/reject",
        "outputs/reject",
        "sources/claim",
        "sources/open",
        "sources/append",
        "sources/finish",
        "sources/close",
        "sources/fail",
        "consumers/open",
        "consumers/read",
        "consumers/complete",
        "consumers/finish",
        "outputs/fail",
    )
)


def subtask_route_allowed(method: str, path: str) -> bool:
    if method == "POST":
        return (
            path in _DATA_POST
            or re.fullmatch(
                rf"/streams/v2/subtasks/{_UUID}/(?:events|lease/renew|lease/release)",
                path,
            )
            is not None
        )
    if method == "GET":
        return (
            re.fullmatch(rf"/streams/v1/{_UUID}(?:/prefix)?", path) is not None
            or re.fullmatch(
                rf"/streams/v2/subtasks/{_UUID}/(?:bootstrap|task-run)", path
            )
            is not None
        )
    return False
