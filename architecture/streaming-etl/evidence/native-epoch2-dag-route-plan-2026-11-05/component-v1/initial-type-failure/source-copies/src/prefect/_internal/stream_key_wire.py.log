"""Pure JSON transport for exact, bounded tagged-key-v1 scalar keys."""

from __future__ import annotations

import base64
import re
from typing import Literal, TypeAlias, TypedDict, cast

from prefect._internal.stream_routing import KeyEncodingError, encode_tagged_key_v1

StreamKey: TypeAlias = None | bool | int | str | bytes


class _NullKeyWire(TypedDict):
    kind: Literal["N"]


class _BooleanKeyWire(TypedDict):
    kind: Literal["B"]
    value: bool


class _IntegerKeyWire(TypedDict):
    kind: Literal["I"]
    value: str


class _StringKeyWire(TypedDict):
    kind: Literal["S"]
    value: str


class _BytesKeyWire(TypedDict):
    kind: Literal["Y"]
    value: str


TaggedKeyWire: TypeAlias = (
    _NullKeyWire | _BooleanKeyWire | _IntegerKeyWire | _StringKeyWire | _BytesKeyWire
)

# A tagged string/byte key reserves five of its 4096 bytes for tag and length.
_MAX_BASE64_TEXT_LENGTH = ((4096 - 5 + 2) // 3) * 4
_DECIMAL_INTEGER = re.compile(r"0|-?[1-9][0-9]*")


def encode_tagged_key_wire_v1(value: StreamKey) -> TaggedKeyWire:
    """Return a fresh JSON object for one exact builtin key.

    An explicit null key is {"kind": "N"}. Absence of an output key is handled
    by the containing terminal request, independently of this codec.
    """
    encode_tagged_key_v1(value)
    if value is None:
        return {"kind": "N"}
    if type(value) is bool:
        return {"kind": "B", "value": value}
    if type(value) is int:
        return {"kind": "I", "value": str(value)}
    if type(value) is str:
        return {"kind": "S", "value": value}
    if type(value) is bytes:
        return {"kind": "Y", "value": base64.b64encode(value).decode("ascii")}
    raise KeyEncodingError("tagged-key-v1 requires an exact builtin scalar")


def decode_tagged_key_wire_v1(wire: object) -> StreamKey:
    """Validate a plain tagged JSON object and return an immutable builtin key.

    Integer and base64 spellings must be canonical. Extra keys, coercions,
    surrogates and oversized encoded keys are refused without echoing input.
    """
    if type(wire) is not dict:
        raise KeyEncodingError("A tagged key must be a plain JSON object")
    mapping = cast(dict[object, object], wire)
    if any(type(key) is not str for key in mapping):
        raise KeyEncodingError("Tagged key object names must be strings")
    kind = mapping.get("kind")
    if type(kind) is not str or kind not in {"N", "B", "I", "S", "Y"}:
        raise KeyEncodingError("Unsupported tagged-key-v1 kind")
    expected = {"kind"} if kind == "N" else {"kind", "value"}
    if mapping.keys() != expected:
        raise KeyEncodingError("Tagged key fields must match its exact kind")
    material = mapping.get("value")
    result: StreamKey
    if kind == "N":
        result = None
    elif kind == "B":
        if type(material) is not bool:
            raise KeyEncodingError("A B key requires an exact JSON boolean")
        result = material
    elif kind == "I":
        if (
            type(material) is not str
            or len(material) > 20
            or _DECIMAL_INTEGER.fullmatch(material) is None
        ):
            raise KeyEncodingError("An I key requires canonical signed-i64 decimal")
        result = int(material)
    elif kind == "S":
        if type(material) is not str:
            raise KeyEncodingError("An S key requires an exact JSON string")
        result = material
    else:
        if type(material) is not str or len(material) > _MAX_BASE64_TEXT_LENGTH:
            raise KeyEncodingError("A Y key requires bounded canonical base64")
        try:
            decoded = base64.b64decode(material.encode("ascii"), validate=True)
        except ValueError as error:
            raise KeyEncodingError(
                "A Y key requires canonical padded base64"
            ) from error
        if base64.b64encode(decoded).decode("ascii") != material:
            raise KeyEncodingError("A Y key requires canonical padded base64")
        result = decoded
    encode_tagged_key_v1(result)
    return result


def canonical_tagged_key_wire_v1(wire: object) -> bytes:
    """Return the original tagged-key-v1 bytes after strict JSON validation."""
    return encode_tagged_key_v1(decode_tagged_key_wire_v1(wire))
