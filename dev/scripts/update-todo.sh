#!/usr/bin/env bash
# Historical helper kept only to fail loudly after the top-level TODO index was subsumed
# into crate SPEC Open Questions. See dev/SPECS_SUBSUMPTION.md.
set -euo pipefail

echo "The top-level specs TODO index has been subsumed into crate SPEC Open Questions." >&2
echo "See dev/SPECS_SUBSUMPTION.md for the audit trail." >&2
exit 1
