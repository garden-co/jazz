---
"jazz-tools": patch
"jazz-rn": patch
---

React Native devices whose store alpha.60 opened without the app's migrations recover. When the server refuses such a store (the new `store_opened_without_migrations` error code), a store holding no unsynced writes is rebuilt and syncs again from the server, with nothing for the app to do. One that does hold writes is kept, and `db.deleteClientStorage()` (which before did nothing on React Native) now deletes the account's native store, keeping the account, so the app can reset it; that drops those writes. See "Upgrading from alpha.60" in the docs.
