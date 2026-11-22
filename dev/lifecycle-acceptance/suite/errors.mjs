// The exact error each language must report. An error that only happens is never a PASS.
// Kinds are each language's own names (drivers/PROTOCOL.md). beta.1 adds no error kind, and no
// rule matches message text (#native-language-apis, 22:37 UTC).

const kinds = (name, byLang) => ({
  name,
  matches: ({ kind }, lang) => byLang[lang] === kind,
});

/** A second open of a held store: Rust's StoreHeld, jazz-ffi's Refused elsewhere. */
export const storeHeld = kinds("StoreHeld (Rust) / Refused", {
  rust: "StoreHeld",
  swift: "refused",
  kotlin: "Refused",
  go: "refused",
});

/** A call the closing ended. */
export const closed = kinds("closed (Rust Stopped)", {
  rust: "Stopped",
  swift: "closed",
  kotlin: "Closed",
  go: "closed",
});

/** A memory store closed with writes the server never got. */
export const unsynced = kinds("unsynced", { rust: "Unsynced", go: "unsynced" });

/** What a language can't do on this handle. */
export const notSupported = kinds("not supported", {
  rust: "Unsupported",
  swift: "notSupported",
  kotlin: "NotSupported",
  go: "notSupported",
});

export const CANCELLED = {
  rust: "driver.cancelled", // the future is dropped
  swift: "cancelled",
  kotlin: "driver.cancelled", // CancellationException
  go: "driver.cancelled", // context.Canceled
};

/** `expected` as it reads in `lang`. */
export const inLang = (expected, lang) => ({
  name: expected.name,
  matches: (reply) => expected.matches(reply, lang),
});
