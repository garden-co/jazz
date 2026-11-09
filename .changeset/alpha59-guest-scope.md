---
"jazz-tools": patch
---

A client scoped to an alpha.59 app (`createJazzClient({ scope: "<alpha.59 app id>" })`, or `<JazzProvider scope="…">`) now finds each local-first guest's alpha.59 account on its device by itself, so guests keep their accounts after the upgrade. The alpha.59 entry is left in place, so a rollback still finds it.
