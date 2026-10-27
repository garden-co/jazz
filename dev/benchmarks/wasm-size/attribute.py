#!/usr/bin/env python3
"""Attribute the bytes of an optimised browser WASM to crates, types and kinds.

Usage: attribute.py OPTIMISED.wasm > report.md

Read the README first: the input must be built with
`-C symbol-mangling-version=v0` so function names carry their generic
arguments, and must be the wasm-opt output, since sizes before wasm-opt do not
predict shipped size. No dependencies beyond the Python standard library.
"""

import collections
import hashlib
import re
import sys

WORKSPACE_CRATES = (
    "jazz",
    "jazz_compression",
    "jazz_db",
    "jazz_model",
    "jazz_protocol",
    "jazz_types",
    "jazz_wasm",
)
STD_CRATES = ("alloc", "core", "hashbrown", "std")
SERDE_CRATES = ("postcard", "serde", "serde_core", "serde_json", "serde_wasm_bindgen")
CRATE_HASH = re.compile(r"\[[0-9a-f]{6,}\]")
OWN_TYPE = re.compile(
    r"\b(" + "|".join(WORKSPACE_CRATES) + r")::((?:[a-z_0-9]+::)*)([A-Z]\w*)"
)


def read_leb(data, offset):
    value = shift = 0
    while True:
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        shift += 7
        if byte < 0x80:
            return value, offset


def read_module(path):
    """Return (functions, data_bytes) with functions as [(name, body bytes)]."""
    data = open(path, "rb").read()
    offset = 8
    imported = 0
    bodies = []
    names = {}
    data_bytes = 0
    while offset < len(data):
        section_id = data[offset]
        size, start = read_leb(data, offset + 1)
        section = data[start : start + size]
        if section_id == 2:
            imported = count_imported_functions(section)
        elif section_id == 10:
            count, cursor = read_leb(section, 0)
            for _ in range(count):
                body_size, cursor = read_leb(section, cursor)
                bodies.append(section[cursor : cursor + body_size])
                cursor += body_size
        elif section_id == 11:
            data_bytes = size
        elif section_id == 0:
            names.update(read_function_names(section))
        offset = start + size
    functions = [
        (CRATE_HASH.sub("", names.get(imported + index, f"func[{imported + index}]")), body)
        for index, body in enumerate(bodies)
    ]
    return functions, data_bytes


def count_imported_functions(section):
    count, cursor = read_leb(section, 0)
    functions = 0
    for _ in range(count):
        for _ in range(2):  # module and field names
            length, cursor = read_leb(section, cursor)
            cursor += length
        kind = section[cursor]
        cursor += 1
        if kind == 0:  # function: type index
            _, cursor = read_leb(section, cursor)
            functions += 1
        elif kind == 1:  # table: element type, limits
            cursor += 1
            flags = section[cursor]
            _, cursor = read_leb(section, cursor + 1)
            if flags & 1:
                _, cursor = read_leb(section, cursor)
        elif kind == 2:  # memory: limits
            flags = section[cursor]
            _, cursor = read_leb(section, cursor + 1)
            if flags & 1:
                _, cursor = read_leb(section, cursor)
        elif kind == 3:  # global: value type, mutability
            cursor += 2
        else:  # tag: attribute, type index
            _, cursor = read_leb(section, cursor + 1)
    return functions


def read_function_names(section):
    length, cursor = read_leb(section, 0)
    if section[cursor : cursor + length] != b"name":
        return {}
    cursor += length
    names = {}
    while cursor < len(section):
        subsection = section[cursor]
        size, start = read_leb(section, cursor + 1)
        if subsection == 1:
            count, inner = read_leb(section, start)
            for _ in range(count):
                index, inner = read_leb(section, inner)
                name_length, inner = read_leb(section, inner)
                names[index] = section[inner : inner + name_length].decode("utf8", "replace")
                inner += name_length
        cursor = start + size
    return names


def path_root(name):
    match = re.match(r"^<*&?(?:mut )?(?:dyn )?\(?([a-z_0-9]+)::", name)
    return match.group(1) if match else "?"


def first_workspace_type(name):
    match = OWN_TYPE.search(name)
    if not match:
        return None, None
    return match.group(1), match.group(1) + "::" + match.group(2) + match.group(3)


def kind_of(name):
    root = path_root(name)
    if "drop_in_place" in name:
        return "drop glue"
    if root in STD_CRATES:
        for needle, kind in (
            ("collections::btree", "std: BTreeMap/Set"),
            ("slice::sort", "std: sort"),
            ("hash", "std: hash maps"),
            ("::fmt", "std: fmt"),
            ("::rc::", "std: Rc/Arc"),
            ("::sync::", "std: Rc/Arc"),
            ("vec::", "std: Vec/iter/slice"),
            ("iter::", "std: Vec/iter/slice"),
            ("slice::", "std: Vec/iter/slice"),
        ):
            if needle in name:
                return kind
        return "std: other"
    if root in SERDE_CRATES or re.search(r"Serialize|Deserialize|Visitor", name):
        return "serde"
    if root in WORKSPACE_CRATES:
        return "own: async fn body" if re.search(r"::\{closure#\d+\}$", name) else "own"
    return "deps: " + root


def owner_of(name):
    """Workspace code by impl type or module; std code by the type that caused it."""
    crates = "|".join(WORKSPACE_CRATES)
    method = re.match(r"^<(?:<)?((?:" + crates + r")::[\w:]+?)(?:<[^>]*>)?(?: as [^>]+)?>::", name)
    if method:
        return "`" + method.group(1).split("::")[-1] + "` methods"
    free = re.match(r"^((?:" + crates + r")::[\w:]+)::\w+", name)
    if free:
        return "free fns in `" + "::".join(free.group(1).split("::")[:3]) + "`"
    return None


def call_targets_masked(body):
    """Body bytes with every `call` immediate zeroed (approximate decoder)."""
    out = bytearray()
    index = 0
    while index < len(body):
        out.append(body[index])
        if body[index] == 0x10:
            index += 1
            while index < len(body) and body[index] & 0x80:
                index += 1
            out.append(0)
        index += 1
    return bytes(out)


def short(name):
    return re.sub(r"(\w+::)+(\w+)", r"\2", name)


def table(lines, header, rows):
    lines.append("| " + " | ".join(header) + " |")
    lines.append("|" + "|".join("---:" if h in ("KB", "%", "fns", "copies", "async KB", "excess KB") else "---" for h in header) + "|")
    for row in rows:
        lines.append("| " + " | ".join(str(cell) for cell in row) + " |")
    lines.append("")


def main():
    functions, data_bytes = read_module(sys.argv[1])
    total = sum(len(body) for _, body in functions)
    kb = lambda size: f"{size / 1024:.0f}"
    pct = lambda size: f"{100 * size / total:.1f}"
    lines = [
        "# Browser WASM attribution",
        "",
        f"Function bodies {total / 1e6:.2f} MB in {len(functions)} functions; data {data_bytes / 1e6:.2f} MB.",
        "",
        "## By kind",
        "",
    ]
    kinds = collections.Counter()
    for name, body in functions:
        kinds[kind_of(name)] += len(body)
    table(lines, ["KB", "%", "kind"], [(kb(v), pct(v), k) for k, v in kinds.most_common(25)])

    lines += ["## Workspace code by impl type or module", ""]
    owners = collections.Counter()
    owner_async = collections.Counter()
    owner_count = collections.Counter()
    for name, body in functions:
        owner = owner_of(name)
        if owner:
            owners[owner] += len(body)
            owner_count[owner] += 1
            if kind_of(name) == "own: async fn body":
                owner_async[owner] += len(body)
    table(
        lines,
        ["KB", "async KB", "fns", "where"],
        [(kb(v), kb(owner_async[k]), owner_count[k], k) for k, v in owners.most_common(30)],
    )

    lines += ["## Workspace types that cause the most std generic code", ""]
    types = collections.Counter()
    type_kinds = collections.defaultdict(collections.Counter)
    for name, body in functions:
        if path_root(name) in STD_CRATES or "drop_in_place" in name:
            _, type_name = first_workspace_type(name)
            if type_name:
                types[type_name] += len(body)
                type_kinds[type_name][kind_of(name)] += len(body)
    table(
        lines,
        ["KB", "type", "mostly"],
        [
            (kb(v), f"`{t}`", ", ".join(f"{k} {kb(x)}" for k, x in type_kinds[t].most_common(3)))
            for t, v in types.most_common(25)
        ],
    )

    lines += ["## Largest functions", ""]
    largest = sorted(functions, key=lambda item: -len(item[1]))[:30]
    table(lines, ["KB", "function"], [(kb(len(b)), f"`{short(n)[:160]}`") for n, b in largest])

    lines += [
        "## Near-duplicates",
        "",
        "Functions of at least 128 bytes whose bodies are identical once call targets are masked.",
        "wasm-opt already merges exact duplicates; these differ only in which functions they call.",
        "",
    ]
    groups = collections.defaultdict(list)
    for name, body in functions:
        if len(body) >= 128:
            groups[hashlib.sha1(call_targets_masked(body)).digest()].append((name, len(body)))
    duplicated = [(size * (len(group) - 1), len(group), group[0][0]) for group in groups.values() for size in [group[0][1]] if len(group) > 1]
    lines.append(f"Excess {kb(sum(e for e, _, _ in duplicated))} KB in {len(duplicated)} groups.")
    lines.append("")
    table(
        lines,
        ["excess KB", "copies", "function"],
        [(kb(e), c, f"`{short(n)[:160]}`") for e, c, n in sorted(duplicated, reverse=True)[:20]],
    )
    print("\n".join(lines))


if __name__ == "__main__":
    main()
