"""Illustrative Liftx stream framing parser; no networking or trading actions.

Not a production SDK: validate returned JSON with the public models and maintain
link scope, generations, revisions, completeness, and bounded client state.
"""
import json
import re
import struct
from decimal import Decimal, InvalidOperation


class WireError(ValueError):
    pass


class Reader:
    def __init__(self, data: bytes):
        self.data = data
        self.offset = 0

    def number(self, fmt: str):
        size = struct.calcsize(fmt)
        if self.offset + size > len(self.data):
            raise WireError("truncated integer")
        value = struct.unpack_from(fmt, self.data, self.offset)[0]
        self.offset += size
        return value

    def string(self, maximum: int):
        end = self.data.find(b"\0", self.offset, self.offset + maximum + 1)
        if end < 0:
            raise WireError("unterminated or overlong string")
        try:
            value = self.data[self.offset:end].decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise WireError("invalid UTF-8") from error
        self.offset = end + 1
        return value

    def finish(self):
        if self.offset != len(self.data):
            raise WireError("trailing bytes")


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise WireError("duplicate JSON field")
        result[key] = value
    return result


def reject_constant(value):
    raise WireError("nonstandard JSON constant")


def decode_frame(data: bytes, *, max_frame_bytes: int):
    """Decode one assembled binary WebSocket message under a caller-owned bound."""
    if not isinstance(data, bytes) or not 7 <= len(data) <= max_frame_bytes:
        raise WireError("invalid frame size/type")
    message_type, length, version, channel = struct.unpack_from("<BIBB", data)
    if length != len(data) - 7 or message_type not in (1, 2, 3, 4):
        raise WireError("invalid header")
    expected_version = 2 if channel == 3 else 1
    if channel not in range(1, 7) or version != expected_version:
        raise WireError("unsupported channel/version")
    payload = data[7:]
    result = {"message_type": message_type, "version": version, "channel": channel}
    if channel in (2, 4, 5, 6):
        try:
            result["payload"] = json.loads(
                payload.decode("utf-8", errors="strict"),
                object_pairs_hook=unique_object,
                parse_float=Decimal,
                parse_constant=reject_constant,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, InvalidOperation) as error:
            raise WireError("invalid JSON payload") from error
        return result
    if channel == 1 and message_type == 3:
        if payload != b"\x01":
            raise WireError("invalid balance readiness marker")
        result["payload"] = {"empty_snapshot_ready": True}
        return result
    reader = Reader(payload)
    rows = []
    if channel == 1:
        if message_type != 1:
            raise WireError("unsupported balance message type")
        count = reader.number("<I")
        if count > (len(payload) - 4) // 10:
            raise WireError("impossible balance row count")
        for _ in range(count):
            asset = reader.string(128)
            if not asset:
                raise WireError("empty asset")
            rows.append({"asset": asset, "free_atomic": str(reader.number("<q")), "free_scale": 12})
        result["payload"] = rows
    else:
        kind = reader.number("<B")
        count = reader.number("<H")
        minimum = 12 if kind == 1 else 8
        if kind not in (1, 2) or count > (len(payload) - 3) // minimum:
            raise WireError("invalid ticker kind/count")
        if (kind == 1 and message_type != 3) or (kind == 2 and message_type != 1):
            raise WireError("invalid ticker kind/type")
        for _ in range(count):
            topic = reader.number("<I")
            if kind == 1:
                exchange = reader.number("<I")
                source = reader.number("<H")
                instrument = reader.string(255)
                if not instrument:
                    raise WireError("empty instrument")
                rows.append({"topic_id": topic, "exchange": exchange, "source_id": source, "instrument_id": instrument})
            else:
                scale = reader.number("<H")
                atomic = reader.string(129)
                if not re.fullmatch(r"-?[0-9]{1,128}", atomic) or scale > 30:
                    raise WireError("invalid price decimal")
                rows.append({"topic_id": topic, "price_atomic": atomic, "price_scale": scale})
        result["payload"] = {"kind": kind, "rows": rows}
    reader.finish()
    return result
