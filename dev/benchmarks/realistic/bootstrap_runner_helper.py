#!/usr/bin/env python3
"""Safe extraction and immutable tree integrity checks for pinned bootstrap archives."""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat
import shutil
import sys
import tarfile

RUNTIME_LINKS = (".runner", ".credentials", ".credentials_rsaparams", ".service", ".env", ".path", "_work", "_diag", "_temp")


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def verify_download(path, expected):
    if len(expected) != 64 or any(char not in "0123456789abcdef" for char in expected):
        raise ValueError("expected digest is malformed")
    if digest(Path(path)) != expected:
        raise ValueError("download digest does not match")


def safe_relative(name):
    path = PurePosixPath(name)
    if (
        not name
        or path.is_absolute()
        or "\\" in name
        or any(ord(char) < 32 or ord(char) == 127 for char in name)
        or any(part in ("", ".", "..") for part in name.split("/"))
    ):
        raise ValueError("archive contains an unsafe path")
    return path


def normalized_target(parent, target):
    link = PurePosixPath(target)
    if not target or link.is_absolute() or "\\" in target or any(ord(char) < 32 or ord(char) == 127 for char in target):
        raise ValueError("archive contains an unsafe link")
    parts = list(parent.parts)
    for part in link.parts:
        if part in ("", "."):
            continue
        if part == "..":
            if not parts:
                raise ValueError("archive link escapes staging directory")
            parts.pop()
        else:
            parts.append(part)
    if not parts:
        raise ValueError("archive link escapes staging directory")
    return PurePosixPath(*parts)


def extract(archive, destination):
    destination = Path(destination)
    if destination.exists() or destination.is_symlink():
        raise ValueError("staging directory must be new")
    with tarfile.open(archive, "r:gz") as bundle:
        members = bundle.getmembers()
        entries = {}
        symlinks = {}
        for member in members:
            rel = safe_relative(member.name)
            name = rel.as_posix()
            if name in entries:
                raise ValueError("archive contains duplicate paths")
            if not (member.isdir() or member.isfile() or member.issym() or member.islnk()):
                raise ValueError("archive contains an unsupported file type")
            entries[name] = member
            if member.issym():
                symlinks[name] = normalized_target(rel.parent, member.linkname).as_posix()
            elif member.islnk():
                symlinks[name] = normalized_target(PurePosixPath(), member.linkname).as_posix()

        # Reject every path that would traverse a symlink, independent of archive order.
        for name in entries:
            parts = PurePosixPath(name).parts
            for length in range(1, len(parts)):
                parent = PurePosixPath(*parts[:length]).as_posix()
                if parent in symlinks:
                    raise ValueError("archive member traverses a symlink parent")

        # Resolve each link chain against the complete archive map; cycles, escapes,
        # and a link whose intermediate component is another link are rejected.
        def resolve_link(name, seen):
            if name in seen:
                raise ValueError("archive contains a cyclic link")
            target = symlinks[name]
            parts = []
            for part in PurePosixPath(target).parts:
                parts.append(part)
                candidate = PurePosixPath(*parts).as_posix()
                if candidate in symlinks:
                    remainder = PurePosixPath(target).parts[len(parts):]
                    resolved = resolve_link(candidate, seen | {name})
                    target = PurePosixPath(resolved, *remainder).as_posix()
                    return resolve_path(target, seen | {name, candidate})
            return resolve_path(target, seen | {name})

        def resolve_path(target, seen):
            parts = PurePosixPath(target).parts
            for length in range(1, len(parts) + 1):
                candidate = PurePosixPath(*parts[:length]).as_posix()
                if candidate in symlinks:
                    resolved = resolve_link(candidate, seen)
                    return PurePosixPath(resolved, *parts[length:]).as_posix()
            return PurePosixPath(*parts).as_posix()

        for name in symlinks:
            resolved = resolve_link(name, set())
            if resolved == ".." or resolved.startswith("../") or resolved.startswith("/"):
                raise ValueError("archive link escapes staging directory")
        destination.mkdir(mode=0o700)
        root = destination.resolve()
        for member in members:
            rel = safe_relative(member.name)
            out = root.joinpath(*rel.parts)
            out.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            if member.isdir():
                out.mkdir(mode=0o700, exist_ok=True)
            elif member.isfile():
                source = bundle.extractfile(member)
                if source is None:
                    raise ValueError("archive file has no contents")
                fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), member.mode & 0o755 or 0o600)
                with os.fdopen(fd, "wb") as target:
                    shutil.copyfileobj(source, target)
            elif member.issym():
                os.symlink(member.linkname, out)
            else:
                target_rel = safe_relative(member.linkname)
                target = root.joinpath(*target_rel.parts)
                if not target.is_file() or target.is_symlink():
                    raise ValueError("archive hard link does not reference an earlier regular file")
                os.link(target, out, follow_symlinks=False)


def _entry(root, path):
    info = path.lstat()
    rel = path.relative_to(root).as_posix()
    mode = info.st_mode & 0o7777
    if path.is_symlink():
        return {"path": rel, "kind": "symlink", "mode": mode, "target": os.readlink(path)}
    if path.is_dir():
        return {"path": rel, "kind": "directory", "mode": mode}
    if path.is_file():
        return {"path": rel, "kind": "file", "mode": mode, "sha256": digest(path)}
    raise ValueError("tree contains an unsupported entry type")


def tree_entries(directory, exclude_runtime=False):
    root = Path(directory)
    entries = []
    for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()):
        rel = path.relative_to(root).as_posix()
        if rel == ".bootstrap-manifest":
            continue
        if exclude_runtime and rel in RUNTIME_LINKS and path.is_symlink():
            continue
        entries.append(_entry(root, path))
    return entries


def make_manifest(directory, manifest, exclude_runtime=False):
    root = Path(directory)
    entries = tree_entries(root, exclude_runtime)
    if not any(entry["kind"] == "file" for entry in entries):
        raise ValueError("archive contains no regular files")
    with Path(manifest).open("x", encoding="utf-8") as output:
        json.dump({"format": 1, "entries": entries}, output, sort_keys=True, separators=(",", ":"))
        output.write("\n")


def verify_manifest(directory, manifest, exclude_runtime=False):
    root = Path(directory)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("tree root is not a real directory")
    try:
        expected = json.loads(Path(manifest).read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("tree manifest is malformed") from error
    if not isinstance(expected, dict) or expected.get("format") != 1 or not isinstance(expected.get("entries"), list):
        raise ValueError("tree manifest is malformed")
    actual = tree_entries(root, exclude_runtime)
    if actual != expected["entries"] or not actual:
        raise ValueError("tree is partial, modified, or contains unmanifested entries")


def validate_state_directories(state, state_owner_uid, group_gid, trusted_parent_uid):
    state = Path(state)
    if not state.is_absolute() or len(state.parts) < 3:
        raise ValueError("runner state path must be absolute and have a protected parent")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    parent_fd = os.open("/", flags)
    state_fd = None
    try:
        for component in state.parts[1:-1]:
            try:
                next_fd = os.open(component, flags, dir_fd=parent_fd)
            except FileNotFoundError:
                return
            os.close(parent_fd)
            parent_fd = next_fd
        parent_info = os.fstat(parent_fd)
        if (not stat.S_ISDIR(parent_info.st_mode) or parent_info.st_uid != trusted_parent_uid
                or parent_info.st_mode & 0o022):
            raise ValueError("runner state parent is unsafe")
        try:
            state_fd = os.open(state.name, flags, dir_fd=parent_fd)
        except FileNotFoundError:
            return
        info = os.fstat(state_fd)
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != state_owner_uid
                or info.st_gid != group_gid or info.st_mode & 0o022):
            raise ValueError("runner state directory is unsafe")
        for name in ("_work", "_diag", "_temp"):
            try:
                child = os.open(name, flags, dir_fd=state_fd)
            except FileNotFoundError:
                continue
            try:
                child_info = os.fstat(child)
                if (not stat.S_ISDIR(child_info.st_mode) or child_info.st_uid != state_owner_uid
                        or child_info.st_gid != group_gid or stat.S_IMODE(child_info.st_mode) != 0o700):
                    raise ValueError(f"runner state directory has unsafe ownership or permissions: {name}")
            finally:
                os.close(child)
    finally:
        if state_fd is not None:
            os.close(state_fd)
        os.close(parent_fd)


def prepare_state_directories(state, state_owner_uid, group_gid, trusted_parent_uid):
    state = Path(state)
    if not state.is_absolute() or len(state.parts) < 3:
        raise ValueError("runner state path must be absolute and have a protected parent")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    parent_fd = os.open("/", flags)
    state_fd = None
    try:
        for component in state.parts[1:-1]:
            next_fd = os.open(component, flags, dir_fd=parent_fd)
            os.close(parent_fd)
            parent_fd = next_fd
        parent_info = os.fstat(parent_fd)
        if (not stat.S_ISDIR(parent_info.st_mode) or parent_info.st_uid != trusted_parent_uid
                or parent_info.st_mode & 0o022):
            raise ValueError("runner state parent is unsafe")
        state_fd = os.open(state.name, flags, dir_fd=parent_fd)
        info = os.fstat(state_fd)
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != state_owner_uid
                or info.st_gid != group_gid or stat.S_IMODE(info.st_mode) != 0o2700):
            raise ValueError("runner state directory is unsafe")
        for name in ("_work", "_diag", "_temp"):
            created = False
            try:
                os.mkdir(name, 0o700, dir_fd=state_fd)
                created = True
            except FileExistsError:
                pass
            child = os.open(name, flags, dir_fd=state_fd)
            try:
                if created:
                    os.fchmod(child, 0o700)
                child_info = os.fstat(child)
                if (not stat.S_ISDIR(child_info.st_mode) or child_info.st_uid != state_owner_uid
                        or child_info.st_gid != group_gid or stat.S_IMODE(child_info.st_mode) != 0o700):
                    raise ValueError(f"runner state directory has unsafe ownership or permissions: {name}")
            finally:
                os.close(child)
    finally:
        if state_fd is not None:
            os.close(state_fd)
        os.close(parent_fd)

def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    extract_parser = commands.add_parser("extract")
    extract_parser.add_argument("archive")
    extract_parser.add_argument("destination")
    manifest_parser = commands.add_parser("manifest")
    manifest_parser.add_argument("directory")
    manifest_parser.add_argument("manifest")
    manifest_parser.add_argument("--exclude-runtime-links", action="store_true")
    verify_parser = commands.add_parser("verify")
    verify_parser.add_argument("directory")
    verify_parser.add_argument("manifest")
    verify_parser.add_argument("--exclude-runtime-links", action="store_true")
    download_parser = commands.add_parser("verify-download")
    download_parser.add_argument("path")
    download_parser.add_argument("expected")
    state_parser = commands.add_parser("prepare-state")
    state_parser.add_argument("directory")
    state_parser.add_argument("owner_uid", type=int)
    state_parser.add_argument("group_gid", type=int)
    state_parser.add_argument("trusted_parent_uid", type=int)
    check_parser = commands.add_parser("check-state")
    check_parser.add_argument("directory")
    check_parser.add_argument("owner_uid", type=int)
    check_parser.add_argument("group_gid", type=int)
    check_parser.add_argument("trusted_parent_uid", type=int)
    args = parser.parse_args()
    try:
        if args.command == "verify-download":
            verify_download(args.path, args.expected)
        elif args.command == "extract":
            extract(args.archive, args.destination)
        elif args.command == "manifest":
            make_manifest(args.directory, args.manifest, args.exclude_runtime_links)
        elif args.command == "verify":
            verify_manifest(args.directory, args.manifest, args.exclude_runtime_links)
        elif args.command == "prepare-state":
            prepare_state_directories(args.directory, args.owner_uid, args.group_gid, args.trusted_parent_uid)
        else:
            validate_state_directories(args.directory, args.owner_uid, args.group_gid, args.trusted_parent_uid)
    except (OSError, ValueError, tarfile.TarError) as error:
        print(f"bootstrap integrity check failed: {error}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
