#!/usr/bin/env bash
# Historical helper kept only to fail loudly after the top-level TODO index was
# retired. GitHub Issues own the work queue; crate SPEC Open Questions link to
# their relevant discussions.
set -euo pipefail

echo "The top-level specs TODO index was retired in favor of GitHub Issues." >&2
echo "Use the linked issue in the relevant crate SPEC Open Questions section." >&2
exit 1
