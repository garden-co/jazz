---
"jazz-tools": patch
---

**Breaking (Rust):** the deprecated `local_first` aliases from alpha.68 are removed. Use the `device_account` names instead:

- `jazz_types::identity::LOCAL_FIRST_ISSUER` → `DEVICE_ACCOUNT_ISSUER`
- `AuthorSubject::LOCAL_FIRST_ISSUER` → `AuthorSubject::DEVICE_ACCOUNT_ISSUER`
- `identity::sign_local_first_challenge` → `sign_device_account_challenge`
- `identity::verify_local_first_challenge` → `verify_device_account_challenge`
- `account_registry::local_first_account_id` → `device_account_id`
- `Binding::admit_local_first_session` → `Binding::admit_device_account_session`
- `Binding::admitted_local_first_accounts` (testing) → `Binding::admitted_device_accounts`
- `jazz_server` `LOCAL_FIRST_JWT_ISSUER` → `DEVICE_ACCOUNT_JWT_ISSUER`
- `jazz_server` `admit_local_first_jwt` → `admit_device_account_jwt`
- `jazz_server` `LOCAL_FIRST_AUDIENCE_HEADER` → `DEVICE_ACCOUNT_AUDIENCE_HEADER`
- `jazz_server` `local_first_audience` → `device_account_audience`
- `jazz_server` `local_first_founding_app` → `device_account_founding_app`

Only the Rust names change. The issuer `urn:jazz:local-first`, the `X-Jazz-Local-First-Audience` header, the `local_first_audience` prelude field, the HTTP error codes and the JS API are unchanged.
