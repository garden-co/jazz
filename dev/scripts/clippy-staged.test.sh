#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/clippy-staged-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

cat >"$tmp/git" <<'EOF'
#!/bin/sh
# Simulate a restricted child process: the hook must not need this executable.
exit 126
EOF
cat >"$tmp/cargo" <<'EOF'
#!/bin/sh
if [ "$1" = clippy ]; then
  exit "${CLIPPY_TEST_EXIT:-0}"
fi
echo "unexpected cargo invocation: $*" >&2
exit 125
EOF
chmod +x "$tmp/git" "$tmp/cargo"

PATH="$tmp:$PATH" JAZZ_REPO_ROOT="$root" \
  sh "$root/dev/scripts/clippy-staged.sh" Cargo.toml >/dev/null

if PATH="$tmp:$PATH" JAZZ_REPO_ROOT="$root" CLIPPY_TEST_EXIT=37 \
  sh "$root/dev/scripts/clippy-staged.sh" Cargo.toml >/dev/null 2>&1; then
  echo "expected cargo failure to propagate" >&2
  exit 1
elif [ "$?" -ne 37 ]; then
  echo "cargo failure was not propagated" >&2
  exit 1
fi

echo "clippy-staged hook tests passed"
