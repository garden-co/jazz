"""Assemble v3 fixtures from frozen v2 fields and explicit Postcard primitives.

No Jazz serializer is imported. Existing JSON must already have identical
values; this script will not replace a pinned fixture with changed bytes.
"""

import copy
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
FIXTURES = HERE.parent
OLD = "4a53514c02"
NEW = "4a53514c03"


def uint(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def string(value):
    encoded = value.encode("utf-8")
    return uint(len(encoded)) + encoded


def rejected(reason, message):
    # Result::Err=1, WireError::Rejected=1, then Option<String> and String.
    optional = b"\x00" if reason is None else b"\x01" + string(reason)
    return b"\x01\x01" + optional + string(message)


def fate(tag, reason, message, *, node=17, tx=127, through=128):
    prefix = bytes.fromhex(NEW) + uint(tag)
    if tag != 14:
        prefix += bytes([node]) * 16
    suffix = b"" if tag == 29 else b"\x00\x01"  # held=[], global=true
    return prefix + uint(tx) + uint(through) + rejected(reason, message) + suffix


def converted(value):
    if isinstance(value, dict):
        return {key: converted(item) for key, item in value.items()}
    if isinstance(value, list):
        return [converted(item) for item in value]
    if isinstance(value, str):
        if value == "jazz.sqlite.sync.v2":
            return "jazz.sqlite.sync.v3"
        if value.startswith(OLD):
            return NEW + value[len(OLD):]
        if "with v2 header" in value:
            return value.replace("with v2 header", "with v3 header")
    return value


def save(path, value):
    if path.exists():
        assert json.loads(path.read_text()) == value, f"refusing to change pinned {path}"
    else:
        path.write_text(json.dumps(value, indent=2) + "\n")


for path in sorted((FIXTURES / "sqlite-sync-v2").glob("*.json")):
    data = converted(json.loads(path.read_text()))
    if "version" in data:
        assert data["version"] == 2
        data["version"] = 3
    if path.name == "relay-upload.json":
        data["fates"]["rejected"] = fate(45, None, "denied").hex()
    save(HERE / path.name, data)

reason_name = "client_clock_too_far_ahead"
cases = []
for name, tag in [("Fate", 14), ("FateV1", 29), ("OriginFateV2", 45)]:
    for reason in [None, reason_name]:
        message = "denied" if reason is None else "client_clock_too_far_ahead: future write"
        fields = {
            "tx": 127,
            "through": 128,
            "result": {"Err": {"Rejected": {"reason": reason, "message": message}}},
        }
        if tag != 14:
            fields["node"] = [17] * 16
        if tag != 29:
            fields.update(held=[], global_=True)
            fields["global"] = fields.pop("global_")
        cases.append({
            "name": f"{name}-{'named' if reason else 'unnamed'}",
            "variant": name,
            "reason": reason,
            "reader": "client",
            "message": {name: fields},
            "frame_hex": fate(tag, reason, message).hex(),
        })
unknown = copy.deepcopy(cases[1])
unknown.update(name="Fate-unknown-reason", reason="future_reason")
unknown["message"]["Fate"]["result"]["Err"]["Rejected"]["reason"] = "future_reason"
unknown["frame_hex"] = fate(14, "future_reason", "client_clock_too_far_ahead: future write").hex()
cases.append(unknown)
save(HERE / "error-cases.json", {"encoding": "jazz.sqlite.sync.v3", "cases": cases})

corpus = converted(json.loads((FIXTURES / "sqlite_sync_wire_v2.json").read_text()))
corpus["format"] = "jazz-sqlite-sync-wire-corpus-v3"
corpus["assembly"] = (
    "JSQL/3 retains all 47 tag numbers and frozen v2 field bytes except Rejected: "
    "Result::Err(1), WireError::Rejected(1), Option<String> reason, String message. "
    "New errors are assembled from explicit unsigned LEB128 and UTF-8 fields in "
    "sqlite-sync-v3/assemble.py. No Jazz serializer generates these pins."
)
for item in corpus["examples"]:
    if item.get("name") == "origin-fate-rejected":
        item["frame_hex"] = fate(45, None, "denied").hex()
    if item.get("name") == "legacy-fate-rejected":
        item["frame_hex"] = fate(29, None, "denied").hex()
corpus["fates"] = [
    {key: case[key] for key in ["name", "reason", "reader", "frame_hex"]}
    for case in [cases[1], cases[0]]
]
for item in corpus["frames"]:
    item["batch_hex"] = item["batch_hex"].replace(OLD, NEW)
for item in corpus["rejections"]:
    item["name"] = item["name"].replace("v2", "v3")
    # Envelope lengths do not change when replacing the five-byte header.
    if item["name"] == "future semantic codec":
        item["batch_hex"] = item["batch_hex"].replace(NEW, "4a53514c04")
    else:
        item["batch_hex"] = item["batch_hex"].replace(OLD, NEW)
old_frame = "4a53514c020101"
corpus["rejections"].append({
    "name": "historical v2 semantic codec", "reader": "server",
    "batch_hex": (b"\x01" + uint(len(bytes.fromhex(old_frame))) + bytes.fromhex(old_frame)).hex(),
})
save(FIXTURES / "sqlite_sync_wire_v3.json", corpus)
artifact_source = (FIXTURES / "sqlite_sync_artifact_corpus_v2.json").read_bytes()
artifact = converted(json.loads(artifact_source))
artifact["role"] = "jazz.sqlite.sync-artifact-corpus.v3"
artifact["version"] = 3
artifact["assembly"] = (
    "All frozen v2 base and extension fields retain their bytes under JSQL/3, "
    "apart from the optional reason added to Rejected. Sources and versionedSources "
    "identify the historical inputs, not the resulting frames. No Jazz serializer "
    "generates these pins. See sqlite-sync-v3/assemble.py."
)
artifact["parentCorpusSha256"] = hashlib.sha256(artifact_source).hexdigest()
for case in artifact["existing_messages"]:
    if case["name"] == "origin_fate_rejected":
        case["frame_hex"] = fate(29, None, "denied").hex()
    if case["name"] == "shared-origin-fate-rejected":
        case["frame_hex"] = fate(45, None, "denied").hex()
save(FIXTURES / "sqlite_sync_artifact_corpus_v3.json", artifact)
save(FIXTURES / "sqlite_write_fate_v2.json", {
    "role": "jazz.sqlite.write-fate.v2", "version": 2,
    "accepted": "00", "conflict": "0100026e6f", "other": "0102026e6f",
    "rejected": rejected(None, "no").hex(),
    "rejected_reason": rejected(reason_name, "no").hex(),
    "rejected_unknown_reason": rejected("future_reason", "no").hex(),
})
