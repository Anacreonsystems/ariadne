"""Pure canonical scalar keys and key groups for `tagged-key-v1`."""

from __future__ import annotations

from hashlib import sha256

_MAX_ENCODED_KEY_BYTES = 4096


class KeyEncodingError(ValueError):
    """A value is outside the exact, bounded `tagged-key-v1` scalar domain."""


def encode_tagged_key_v1(value: None | bool | int | str | bytes) -> bytes:
    """Encode an exact built-in scalar without coercion or normalization.

    The encoded representation includes its type tag and, for strings and
    bytes, a four-byte unsigned length. Integers use signed 64-bit big-endian
    representation. The complete key cannot exceed 4096 bytes.
    """
    if value is None:
        return b"N"
    if type(value) is bool:
        return b"B\x01" if value else b"B\x00"
    if type(value) is int:
        if not -(1 << 63) <= value < (1 << 63):
            raise KeyEncodingError("tagged-key-v1 integers must fit signed 64 bits")
        return b"I" + value.to_bytes(8, "big", signed=True)

    payload_limit = _MAX_ENCODED_KEY_BYTES - 5
    if type(value) is str:
        if len(value) > payload_limit:
            raise KeyEncodingError(
                "tagged-key-v1 keys cannot exceed 4096 encoded bytes"
            )
        try:
            payload = value.encode("utf-8", errors="strict")
        except UnicodeEncodeError as exc:
            raise KeyEncodingError(
                "tagged-key-v1 strings require strict UTF-8"
            ) from exc
        tag = b"S"
    elif type(value) is bytes:
        payload = value
        tag = b"Y"
    else:
        raise KeyEncodingError(
            "tagged-key-v1 requires exact None, bool, int, str, or bytes values"
        )

    if len(payload) > payload_limit:
        raise KeyEncodingError("tagged-key-v1 keys cannot exceed 4096 encoded bytes")
    return tag + len(payload).to_bytes(4, "big") + payload


def tagged_key_v1_group(
    value: None | bool | int | str | bytes, key_group_count: int
) -> int:
    """Map a canonical key's full unsigned SHA-256 digest to a positive group count.

    Operational limits on the persisted group count belong to profile
    admission. This pure primitive accepts any exact positive Python integer.
    """
    if type(key_group_count) is not int or key_group_count <= 0:
        raise ValueError("key_group_count must be an exact positive integer")
    digest = sha256(encode_tagged_key_v1(value)).digest()
    return int.from_bytes(digest, "big", signed=False) % key_group_count
