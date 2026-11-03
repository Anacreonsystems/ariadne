"""Deterministic code identity for native stream task reconstruction."""

from __future__ import annotations

import inspect
import json
import math
from dataclasses import dataclass
from hashlib import sha256
from types import CodeType, FunctionType
from typing import Any, Callable, cast


class NativeTaskCodeError(ValueError):
    """A callable cannot be represented by the native code protocol."""


@dataclass(frozen=True)
class NativeTaskCodeIdentity:
    """Stable task key and full structural code digest."""

    task_key: str
    artifact_digest: str


def _encoded(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _constant(value: Any) -> list[Any]:
    if value is None:
        return ["none"]
    if value is Ellipsis:
        return ["ellipsis"]
    if type(value) is bool:
        return ["bool", value]
    if type(value) is int:
        return ["int", str(value)]
    if type(value) is float:
        # `float.hex` retains signed zero and exact finite values. Explicit
        # spellings keep non-finite constants deterministic as well.
        if math.isnan(value):
            encoded = "nan"
        elif value == math.inf:
            encoded = "+inf"
        elif value == -math.inf:
            encoded = "-inf"
        else:
            encoded = value.hex()
        return ["float", encoded]
    if type(value) is complex:
        return ["complex", _constant(value.real)[1], _constant(value.imag)[1]]
    if type(value) is str:
        return ["str", value]
    if type(value) is bytes:
        return ["bytes", value.hex()]
    if type(value) is tuple:
        items = cast(tuple[Any, ...], value)
        return ["tuple", [_constant(item) for item in items]]
    if type(value) is frozenset:
        items = cast(frozenset[Any], value)
        members = [_constant(item) for item in items]
        members.sort(key=_encoded)
        return ["frozenset", members]
    if isinstance(value, CodeType):
        return ["code", _code(value)]
    raise NativeTaskCodeError(
        f"Unsupported native task code constant: {type(value).__name__}"
    )


def _code(code: CodeType) -> dict[str, Any]:
    """Encode semantic code structure without paths or debug line tables."""

    return {
        "argcount": code.co_argcount,
        "posonlyargcount": code.co_posonlyargcount,
        "kwonlyargcount": code.co_kwonlyargcount,
        "nlocals": code.co_nlocals,
        "stacksize": code.co_stacksize,
        "flags": code.co_flags,
        # `co_code` exposes the unspecialized instruction stream even after
        # CPython has quickened a live function internally.
        "code": code.co_code.hex(),
        "exceptiontable": code.co_exceptiontable.hex(),
        "consts": [_constant(item) for item in code.co_consts],
        "names": list(code.co_names),
        "varnames": list(code.co_varnames),
        "freevars": list(code.co_freevars),
        "cellvars": list(code.co_cellvars),
        "name": code.co_name,
        "qualname": code.co_qualname,
    }


def native_task_code_identity(fn: Callable[..., Any]) -> NativeTaskCodeIdentity:
    """Return process-stable identity for a supported native task function.

    Native reconstruction requires an ordinary top-level Python function.
    Closure values and callable objects have no process-independent structural
    representation in this first profile, so they fail before admission.
    Function defaults are represented by the already canonicalized submission
    parameters; the explicit task version remains the user contract for global
    dependencies outside the function's code object.
    """

    if not isinstance(fn, FunctionType) or not inspect.isfunction(fn):
        raise NativeTaskCodeError(
            "Native stream tasks require an ordinary Python function"
        )
    if fn.__closure__ or fn.__code__.co_freevars or "<locals>" in fn.__qualname__:
        raise NativeTaskCodeError(
            "Native stream tasks do not support closures or local functions"
        )
    qualname = fn.__qualname__
    if not qualname:
        raise NativeTaskCodeError("Native stream task function has no qualname")
    digest = sha256(
        _encoded(
            {
                "schema": "prefect-native-task-code-v1",
                "code": _code(fn.__code__),
            }
        )
    ).hexdigest()
    # Keep the familiar readable task-key form and the server's 255-character
    # bound while retaining the full digest separately in the manifest.
    task_key = f"{qualname[:246]}-{digest[:8]}"
    return NativeTaskCodeIdentity(task_key=task_key, artifact_digest=digest)
