// The exact error each language must report. An error that only happens is never a PASS.
// Kinds are each language's own names (drivers/PROTOCOL.md).

// crates/jazz-app's Error::StoreHeld reads "the local store is held: …". jazz-ffi reports it as
// Refused with that message, so outside Rust it takes the kind and the message to tell it from
// every other refusal (and from `database is locked`, which is Io).
const HELD = /the local store is held/;

export const storeHeld = {
  name: "StoreHeld",
  matches({ kind, msg }, lang = this.lang) {
    switch (lang) {
      case "rust":
        return kind === "StoreHeld";
      case "swift":
      case "go":
        return kind === "storeHeld" || (kind === "refused" && HELD.test(msg));
      case "kotlin":
        return kind === "StoreHeld" || (kind === "Refused" && HELD.test(msg));
    }
    return false;
  },
};

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
