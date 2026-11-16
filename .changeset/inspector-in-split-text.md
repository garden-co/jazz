---
"jazz-tools": patch
---

The Inspector's IN / NOT IN filter splits Text, Enum and other non-JSON values on every comma again, so a stray quote or bracket (`O"Neil, Smith`) no longer stops the split. JSON and array values still split only on commas outside strings and brackets.
