---
"jazz-tools": patch
---

A migration can now add variants to an enum column (or an array of enums), at the end of its list. `jazz-tools migrations create` writes a stub with nothing to fill in, and `validate` and deploys accept it. Clients still on the older schema read a row holding the new variant with its name as it is; their own writes are still checked against their own variants. Renaming or removing an enum variant is still refused, now naming the variant, and so is reordering the list or adding a variant before its end, which would change how existing rows sort.
