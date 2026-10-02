---
"jazz-tools": patch
---

Record verified E2EE initialization promotion before recipient delivery. Interrupted delivery still fails the Global wait, but a fresh database can resume eligible handoff without treating accepted initialization as missing original-owner work. Historical rotated and sealed spaces are not swept for initial delivery.
