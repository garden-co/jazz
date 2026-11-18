---
"jazz-tools": patch
---

**Breaking (Rust):** the device-account (formerly "local-first auth") APIs use `device_account` names: `AuthAdmissionConfig.allow_device_accounts`, `AdmissionSource::DeviceAccountJwt`, `admit_device_account_jwt`, `sign_device_account_challenge` / `verify_device_account_challenge`, `device_account_audience` / `device_account_founding_app`, `Binding::admit_device_account_session`, `device_account_id`, `AccountCommand::FoundDeviceAccount`, and the `DEVICE_ACCOUNT_ISSUER`, `DEVICE_ACCOUNT_JWT_ISSUER` and `DEVICE_ACCOUNT_AUDIENCE_HEADER` constants. The old function and constant names stay as deprecated aliases for one release; the renamed field and enum variants have none. Wire formats, signed values, HTTP error codes and the JS API are unchanged.
