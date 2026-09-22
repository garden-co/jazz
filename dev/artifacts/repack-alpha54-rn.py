#!/usr/bin/env python3
"""One-off, hash-pinned alpha.54 RN debug-only packaging recovery; never publishes."""
import argparse
import copy
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import subprocess
import tarfile
import tempfile

SOURCE = '71676612e331d7bc0f554a8e0fcba244d9b786ec'
INPUT_SHA256 = '0a98d21ec43a19e27c26cfea3db0213881219b6e412e03a94a2fe62528a6426e'
VERSION = '2.0.0-alpha.54'


def digest(path):
    with open(path, 'rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path)
    parser.add_argument('output', type=Path, help='New output directory')
    parser.add_argument('--source-root', type=Path, required=True)
    args = parser.parse_args()
    source = args.source_root.resolve()
    if run('git', '-C', str(source), 'rev-parse', 'HEAD', capture_output=True, text=True).stdout.strip() != SOURCE:
        raise RuntimeError('source checkout must be the exact alpha.54 commit')
    if run('git', '-C', str(source), 'status', '--porcelain', capture_output=True, text=True).stdout:
        raise RuntimeError('source checkout must be clean')
    if digest(args.input) != INPUT_SHA256:
        raise RuntimeError('input tarball differs from trusted release artifact')
    strip_version = run('llvm-strip-18', '--version', capture_output=True, text=True).stdout
    args.output.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, JAZZ_NATIVE_RELAY_SOURCE_REVISION=SOURCE,
               JAZZ_NATIVE_RELAY_CARGO_NDK_VERSION='4.1.2')
    receipt = dict(sourceRevision=SOURCE, inputSha256=INPUT_SHA256,
                   inputRun=34428566341, transform='llvm-strip-18 --strip-debug; gzip level 9',
                   toolVersion=strip_version, archives=[])
    with tempfile.TemporaryDirectory(prefix='alpha54-rn-') as scratch:
        root = Path(scratch)
        with tarfile.open(args.input, 'r:gz') as tar:
            members = tar.getmembers()
            names = set()
            for member in members:
                path = PurePosixPath(member.name)
                if path.is_absolute() or '..' in path.parts or not path.parts or path.parts[0] != 'package':
                    raise RuntimeError('unsafe archive path')
                if member.name in names or not (member.isfile() or member.isdir()):
                    raise RuntimeError('duplicate or unsupported archive member')
                names.add(member.name)
            tar.extractall(root, filter='data')
        package = root / 'package'
        metadata = json.loads((package / 'package.json').read_text())
        if (metadata['name'], metadata['version']) != ('jazz-rn', VERSION):
            raise RuntimeError('unexpected package identity')
        def verify():
            run('node', str(source / 'crates/jazz-rn/scripts/verify-relay-artifacts.mjs'),
                'android', 'ios', '--package-root', str(package), env=env)
        verify()
        before = {p.relative_to(package).as_posix(): digest(p) for p in package.rglob('*') if p.is_file()}
        archives = sorted(package.rglob('*.a'))
        expected = {
            'android/src/main/jniLibs/' + abi + '/libjazz_native_relay.a'
            for abi in ('arm64-v8a', 'armeabi-v7a', 'x86_64')
        } | {'JazzNativeRelay.xcframework/' + abi + '/libjazz_native_relay.a'
             for abi in ('ios-arm64', 'ios-arm64_x86_64-simulator')}
        if {p.relative_to(package).as_posix() for p in archives} != expected:
            raise RuntimeError('unexpected native archive inventory')
        for archive in archives:
            def symbols():
                # Embedded Rust bitcode is newer than LLVM18; native symbols are
                # inspected without asking LLVM18 to deserialize that bitcode.
                return run('llvm-nm-18', '--no-llvm-bc', '--arch=all', '--extern-only',
                           '--format=posix', str(archive), capture_output=True).stdout
            old_symbols = symbols()
            entry = dict(path=archive.relative_to(package).as_posix(),
                         beforeSha256=digest(archive), beforeBytes=archive.stat().st_size)
            run('llvm-strip-18', '--strip-debug', str(archive))
            if symbols() != old_symbols:
                raise RuntimeError('global native symbols changed: ' + str(archive))
            entry.update(afterSha256=digest(archive), afterBytes=archive.stat().st_size,
                         nativeGlobalSymbolsSha256=hashlib.sha256(old_symbols).hexdigest())
            receipt['archives'].append(entry)
        manifests = {'android/jazz-native-relay.manifest.json': 'android/src/main/jniLibs',
                     'ios/jazz-native-relay.manifest.json': 'JazzNativeRelay.xcframework'}
        for name, base in manifests.items():
            path = package / name
            manifest = json.loads(path.read_text())
            for entry in manifest['files']:
                entry['sha256'] = digest(package / base / entry['path'])
            path.write_text(json.dumps(manifest, indent=2) + '\n')
        verify()
        after = {p.relative_to(package).as_posix(): digest(p) for p in package.rglob('*') if p.is_file()}
        if before.keys() != after.keys() or any(before[p] != after[p] for p in before.keys() - expected - manifests.keys()):
            raise RuntimeError('unexpected package content changes')
        output = args.output / ('jazz-rn-' + VERSION + '.tgz')
        with output.open('wb') as raw, gzip.GzipFile(filename='', mode='wb', fileobj=raw, compresslevel=9, mtime=0) as gz:
            with tarfile.open(fileobj=gz, mode='w|', format=tarfile.PAX_FORMAT) as tar:
                for original in members:
                    member = copy.copy(original)
                    if member.isfile():
                        path = root / member.name
                        member.size = path.stat().st_size
                        with path.open('rb') as payload:
                            tar.addfile(member, payload)
                    else:
                        tar.addfile(member)
        if output.stat().st_size >= 195000000:
            raise RuntimeError('repacked artifact exceeds 195000000 byte ceiling')
        receipt.update(outputSha256=digest(output), outputBytes=output.stat().st_size,
                       manifests={name: json.loads((package / name).read_text()) for name in manifests})
        (args.output / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
        print(json.dumps({'outputBytes': receipt['outputBytes'], 'outputSha256': receipt['outputSha256']}))


if __name__ == '__main__':
    main()
