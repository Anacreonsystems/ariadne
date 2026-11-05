"""Isolated scalar key transport checks; no database or runtime execution."""

import base64
import json
import subprocess
import sys
import unittest
from enum import IntEnum
from pathlib import Path
from types import MappingProxyType

from prefect._internal.stream_key_wire import (
    canonical_tagged_key_wire_v1,
    decode_tagged_key_wire_v1,
    encode_tagged_key_wire_v1,
)
from prefect._internal.stream_routing import KeyEncodingError, encode_tagged_key_v1


class TestStreamKeyWire(unittest.TestCase):
    def test_independent_wire_vectors_and_byte_codec_agree(self):
        vectors = [
            (None, {"kind": "N"}),
            (False, {"kind": "B", "value": False}),
            (True, {"kind": "B", "value": True}),
            (0, {"kind": "I", "value": "0"}),
            (-1, {"kind": "I", "value": "-1"}),
            (-(1 << 63), {"kind": "I", "value": "-9223372036854775808"}),
            ((1 << 63) - 1, {"kind": "I", "value": "9223372036854775807"}),
            ("", {"kind": "S", "value": ""}),
            ("é", {"kind": "S", "value": "é"}),
            ("e\u0301", {"kind": "S", "value": "e\u0301"}),
            ("\0", {"kind": "S", "value": "\0"}),
            (b"", {"kind": "Y", "value": ""}),
            (b"f", {"kind": "Y", "value": "Zg=="}),
            (b"\x66\x6f", {"kind": "Y", "value": "Zm8="}),
            (b"foo", {"kind": "Y", "value": "Zm9v"}),
            (b"\0\xff", {"kind": "Y", "value": "AP8="}),
        ]
        for value, expected in vectors:
            with self.subTest(value_type=type(value).__name__, wire=expected):
                encoded = encode_tagged_key_wire_v1(value)
                self.assertEqual(encoded, expected)
                transported = json.loads(json.dumps(encoded))
                decoded = decode_tagged_key_wire_v1(transported)
                self.assertIs(type(decoded), type(value))
                self.assertEqual(decoded, value)
                self.assertEqual(
                    canonical_tagged_key_wire_v1(transported),
                    encode_tagged_key_v1(value),
                )

    def test_boolean_integer_string_and_bytes_keep_distinct_identity(self):
        values = (False, 0, "0", b"0", None, "N", b"N")
        encoded = [
            canonical_tagged_key_wire_v1(encode_tagged_key_wire_v1(v)) for v in values
        ]
        self.assertEqual(len(set(encoded)), len(values))

    def test_explicit_null_is_not_an_absent_key_object(self):
        self.assertIsNone(decode_tagged_key_wire_v1({"kind": "N"}))
        for wire in (None, {}, {"kind": "N", "value": None}):
            with self.subTest(wire=wire):
                with self.assertRaises(KeyEncodingError):
                    decode_tagged_key_wire_v1(wire)

    def test_extra_and_missing_fields_are_refused(self):
        for wire in (
            {"kind": "B"},
            {"value": True},
            {"kind": "I", "value": "1", "codec": "tagged-key-v1"},
            {"kind": "S", "value": "a", "other": None},
            {"kind": "Y", "value": "", "kind2": "Y"},
            {"kind": "N", "extra": 0},
        ):
            with self.subTest(wire=wire):
                with self.assertRaises(KeyEncodingError):
                    canonical_tagged_key_wire_v1(wire)

    def test_unknown_or_coerced_kinds_are_refused(self):
        for kind in ("n", "null", "", "UNKNOWN", 0, True, None, b"N"):
            with self.subTest(kind=kind):
                with self.assertRaises(KeyEncodingError):
                    decode_tagged_key_wire_v1({"kind": kind})

    def test_boolean_values_require_exact_json_booleans(self):
        for value in (0, 1, 0.0, 1.0, "true", "false", None, [], {}):
            with self.subTest(value=value):
                with self.assertRaises(KeyEncodingError):
                    decode_tagged_key_wire_v1({"kind": "B", "value": value})

    def test_integer_decimal_spelling_is_canonical(self):
        for value in (
            "-0",
            "+0",
            "+1",
            "00",
            "01",
            "-01",
            "1.0",
            "1e0",
            " 1",
            "1 ",
            "1\n",
            "\t1",
            "١",
            "１",
            "",
            "--1",
            str(1 << 63),
            str(-(1 << 63) - 1),
            "1" * 100_000,
            0,
            1,
            True,
            None,
        ):
            with self.subTest(
                value_type=type(value).__name__,
                length=len(value) if type(value) is str else None,
            ):
                with self.assertRaises(KeyEncodingError):
                    decode_tagged_key_wire_v1({"kind": "I", "value": value})

    def test_strings_require_exact_utf8_without_surrogates(self):
        for value in (None, 1, True, b"a", ["a"], {"text": "a"}, "\ud800", "\udfff"):
            with self.subTest(value_type=type(value).__name__):
                with self.assertRaises(KeyEncodingError):
                    decode_tagged_key_wire_v1({"kind": "S", "value": value})
        first = canonical_tagged_key_wire_v1({"kind": "S", "value": "é"})
        second = canonical_tagged_key_wire_v1({"kind": "S", "value": "e\u0301"})
        self.assertNotEqual(first, second)

    def test_base64_requires_exact_standard_canonical_padding_and_bits(self):
        for value in (
            "Zg",
            "Zg=",
            "Zg===",
            "Zh==",
            "Zm9=",
            "AA==\n",
            " AA==",
            "AA== ",
            "AA-_",
            "AA-_==",
            "AA==AA==",
            "A",
            "é",
            "\ud800",
            None,
            b"AA==",
            0,
            True,
            "A" * 100_000,
        ):
            with self.subTest(
                value_type=type(value).__name__,
                length=len(value) if type(value) is str else None,
            ):
                with self.assertRaises(KeyEncodingError):
                    decode_tagged_key_wire_v1({"kind": "Y", "value": value})

    def test_tagged_4096_byte_boundary_is_shared_with_existing_codec(self):
        for value in ("a" * 4091, "é" * 2045 + "a", b"a" * 4091):
            with self.subTest(value_type=type(value).__name__):
                wire = encode_tagged_key_wire_v1(value)
                self.assertEqual(len(canonical_tagged_key_wire_v1(wire)), 4096)
                self.assertEqual(decode_tagged_key_wire_v1(wire), value)
        for value in ("a" * 4092, "é" * 2046, b"a" * 4092):
            with self.subTest(value_type=type(value).__name__):
                with self.assertRaises(KeyEncodingError):
                    encode_tagged_key_wire_v1(value)
                wire = (
                    {"kind": "Y", "value": base64.b64encode(value).decode()}
                    if type(value) is bytes
                    else {"kind": "S", "value": value}
                )
                with self.assertRaises(KeyEncodingError):
                    canonical_tagged_key_wire_v1(wire)

    def test_custom_scalar_subtypes_and_unsupported_keys_are_refused(self):
        class IntegerKind(IntEnum):
            ONE = 1

        class Text(str):
            pass

        class Bytes(bytes):
            pass

        for value in (
            IntegerKind.ONE,
            Text("a"),
            Bytes(b"a"),
            1.0,
            -0.0,
            float("inf"),
            float("nan"),
            [],
            {},
            object(),
        ):
            with self.subTest(value_type=type(value).__name__):
                with self.assertRaises(KeyEncodingError):
                    encode_tagged_key_wire_v1(value)
        for wire in (
            {"kind": Text("N")},
            {"kind": "I", "value": Text("1")},
            {"kind": "S", "value": Text("a")},
        ):
            with self.assertRaises(KeyEncodingError):
                decode_tagged_key_wire_v1(wire)

    def test_parser_accepts_only_plain_json_objects_without_callbacks(self):
        class UntrustedDictionary(dict):
            def __iter__(self):
                raise AssertionError("A non-JSON object callback was invoked")

        for wire in (
            [],
            "N",
            ("N",),
            MappingProxyType({"kind": "N"}),
            UntrustedDictionary(kind="N"),
            {1: "N"},
        ):
            with self.subTest(value_type=type(wire).__name__):
                with self.assertRaises(KeyEncodingError):
                    decode_tagged_key_wire_v1(wire)

    def test_wire_and_parsed_values_share_no_mutable_state(self):
        first = encode_tagged_key_wire_v1("original")
        decoded = decode_tagged_key_wire_v1(first)
        canonical = canonical_tagged_key_wire_v1(first)
        first["value"] = "mutated"  # type: ignore[typeddict-unknown-key]
        second = encode_tagged_key_wire_v1("original")
        self.assertEqual(decoded, "original")
        self.assertEqual(second, {"kind": "S", "value": "original"})
        self.assertEqual(canonical, encode_tagged_key_v1("original"))

    def test_errors_do_not_echo_untrusted_key_material(self):
        secret = "private-key-material"
        with self.assertRaises(KeyEncodingError) as error:
            decode_tagged_key_wire_v1({"kind": "Y", "value": secret})
        self.assertNotIn(secret, str(error.exception))

    def test_module_and_codec_work_with_stdlib_only_in_fresh_process(self):
        source = Path(__file__).resolve().parents[1] / "src/prefect"
        script = """
import importlib.abc, json, sys, types
from pathlib import Path
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'pydantic' or fullname.startswith(('pydantic.', 'prefect.server')):
            raise ImportError('Forbidden non-stdlib key-codec dependency')
sys.meta_path.insert(0, Guard())
root=Path(sys.argv[1])
package=types.ModuleType('prefect'); package.__path__=[str(root)]
internal=types.ModuleType('prefect._internal'); internal.__path__=[str(root/'_internal')]
sys.modules['prefect']=package; sys.modules['prefect._internal']=internal
from prefect._internal.stream_key_wire import encode_tagged_key_wire_v1, canonical_tagged_key_wire_v1
from prefect._internal.stream_routing import encode_tagged_key_v1
for key in [None,False,0,-1,'é',b'\\0\\xff']:
    assert canonical_tagged_key_wire_v1(encode_tagged_key_wire_v1(key)) == encode_tagged_key_v1(key)
print(json.dumps({'stdlib_only':True,'accepted_keys':6,'server_imports':0}))
"""
        process = subprocess.run(
            [sys.executable, "-I", "-S", "-c", script, str(source)],
            capture_output=True,
            text=True,
            timeout=15,
        )
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(
            json.loads(process.stdout),
            {"stdlib_only": True, "accepted_keys": 6, "server_imports": 0},
        )


if __name__ == "__main__":
    unittest.main()
