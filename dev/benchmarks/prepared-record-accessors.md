# Prepared physical field accessor trial

This tests an execution-only change on the existing lowered graphs: resolve
fixed field ranges and variable offset-table slots while preparing a projection,
then reuse those constants across input rows. The existing encoded layout and
record validity checks are preserved. Nested fields prepare one accessor for
each level; constant/evaluation errors remain lazy.

Unlike #2797, this prepares physical span access, not just field names, types and
nested descriptor paths. It does not repeat the rejected fresh-join constructor
#2864. General descriptor field access outside prepared projections is unchanged.

Initial validation: full Groove library suite passes (757 passed, 2 ignored).
An internal byte-boundary oracle compares prepared spans with the general
field-span implementation across fixed/mixed/variable records, every truncation,
extra trailing bytes and invalid offset-table entries. Existing projection tests
cover nested fields, nullability, mixed evaluation and rollback.

Alternating optimized native full-load measurements and scoped follow-up
profiles are pending. No speedup or release readiness is claimed yet.
