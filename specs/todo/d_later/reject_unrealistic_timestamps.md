# Reject Unrealistic Timestamps — TODO (Later)

Ability to reject commits with unrealistic timestamps based on server-side received-at time.

## Overview

Clients set their own timestamps on commits. A malicious or buggy client could send commits with timestamps far in the past or future, disrupting LWW merge ordering. The server should be able to reject or flag commits whose claimed timestamp diverges too far from the server's received-at time.

## Open Questions

- Threshold — how much drift is acceptable? (clock skew vs. offline-then-sync scenarios)
- Action on violation — hard reject, or accept but flag/log?
- Should offline clients that sync hours/days later be treated differently?
- Does this interact with time-travel queries or commit history?
