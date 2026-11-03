from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from prefect._internal.stream_code import (
    NativeTaskCodeError,
    native_task_code_identity,
)


def _identity_in_subprocess(seed: str) -> dict[str, dict[str, str]]:
    fixture = Path(__file__).parent / "fixtures" / "native_stream_document_flow.py"
    script = """
import json
import sys
from pathlib import Path

from prefect._internal.stream_code import native_task_code_identity
from prefect.flows import load_flow_from_entrypoint

path = Path(sys.argv[1]).resolve()
load_flow_from_entrypoint(str(path) + ':documents_flow')
module = next(
    module
    for module in sys.modules.values()
    if getattr(module, '__file__', None) == str(path)
)
print(json.dumps({
    name: native_task_code_identity(getattr(module, name).fn).__dict__
    for name in ('petya', 'anya')
}, sort_keys=True))
"""
    environment = dict(os.environ, PYTHONHASHSEED=seed)
    result = subprocess.run(
        [sys.executable, "-c", script, str(fixture)],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    return json.loads(result.stdout)


def _function(source: str, filename: str = "operator.py"):
    namespace: dict[str, object] = {}
    exec(compile(source, filename, "exec"), namespace)
    return namespace["operator"]


def test_native_code_identity_is_stable_across_process_hash_seeds():
    identities = [_identity_in_subprocess(seed) for seed in ("1", "2", "7919")]

    assert identities[1:] == identities[:-1]
    assert identities[0]["anya"]["task_key"].startswith("anya-")
    assert len(identities[0]["anya"]["artifact_digest"]) == 64


def test_native_code_identity_ignores_debug_location():
    first = _function(
        """
def operator(value):
    return value in {'alpha', 'beta', 'gamma'}
""",
        "/first/location.py",
    )
    second = _function(
        """



def operator(value):
    return value in {'alpha', 'beta', 'gamma'}
""",
        "/another/location.py",
    )

    # Keep source unchanged: compiler constant tables may retain literal-order
    # residue even when two rewritten sources execute equivalent instructions.
    # Hash-seed/frozenset order is covered by the fresh-process control above.
    assert first.__code__.co_filename != second.__code__.co_filename
    assert first.__code__.co_firstlineno != second.__code__.co_firstlineno
    assert native_task_code_identity(first) == native_task_code_identity(second)


@pytest.mark.parametrize(
    "mutation",
    [
        "return value in {'alpha', 'beta', 'delta'}",
        "return value not in {'alpha', 'beta', 'gamma'}",
        "return value in {'alpha', 'beta', 'gamma'} or value == 'omega'",
    ],
)
def test_native_code_identity_detects_semantic_mutations(mutation: str):
    original = _function(
        """
def operator(value):
    return value in {'alpha', 'beta', 'gamma'}
"""
    )
    changed = _function(f"def operator(value):\n    {mutation}\n")

    assert native_task_code_identity(original) != native_task_code_identity(changed)


def test_native_code_identity_is_unchanged_after_interpreter_specialization():
    operator = _function(
        """
def operator(value):
    return value + 1
"""
    )
    before = native_task_code_identity(operator)

    for value in range(20_000):
        operator(value)

    assert native_task_code_identity(operator) == before


def test_native_code_identity_rejects_nonportable_callable_shapes():
    class CallableObject:
        def __call__(self, value):
            return value

    def enclosing():
        captured = 1

        def operator(value):
            return value + captured

        return operator

    with pytest.raises(NativeTaskCodeError, match="ordinary Python function"):
        native_task_code_identity(CallableObject())
    with pytest.raises(NativeTaskCodeError, match="closures or local functions"):
        native_task_code_identity(enclosing())
