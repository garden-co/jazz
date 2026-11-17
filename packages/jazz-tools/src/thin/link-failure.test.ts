import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { classifyWireLinkFailure } from "../runtime/native-runtime/websocket.js";

type Case = { code: string; retry: string; message: string; admitted: boolean; kind: string };

const table = JSON.parse(
  readFileSync(
    new URL(
      "../../../../crates/jazz/layers/db/fixtures/link_wire_failure_v1.json",
      import.meta.url,
    ),
    "utf8",
  ),
) as { cases: Case[] };

// The thin client loads no wasm, so it cannot ask the Rust link how to treat a
// server's error. It is held to the table Rust's classifier is checked
// against (`LinkFailure::from_wire_error`), case for case.
describe("thin client wire failure classification", () => {
  it.each(table.cases)("$code/$retry admitted=$admitted ($message) is $kind", (c) => {
    expect(
      classifyWireLinkFailure({ code: c.code, retry: c.retry, message: c.message }, c.admitted),
    ).toBe(c.kind);
  });

  it("retries a code or retry from a newer server, never stops on it", () => {
    expect(classifyWireLinkFailure({ code: "unknown:9", retry: "never", message: "" }, false)).toBe(
      "refused",
    );
    expect(
      classifyWireLinkFailure({ code: "not_ready", retry: "unknown:4", message: "" }, false),
    ).toBe("refused");
  });

  it("stops when it could not read what the server sent, or its socket ended for good", () => {
    expect(
      classifyWireLinkFailure({ code: "protocol_error", retry: "never", message: "" }, true),
    ).toBe("fatal");
    expect(
      classifyWireLinkFailure({ code: "websocket_closed", retry: "never", message: "" }, true),
    ).toBe("fatal");
  });

  it("retries a refusal with a retry it doesn't know, as native clients do", () => {
    expect(
      classifyWireLinkFailure({ code: "auth_failed", retry: "unknown:9", message: "" }, false),
    ).toBe("refused");
  });

  it("treats its own socket drops as an outage", () => {
    for (const code of ["websocket_closed", "websocket_error", "websocket_silent"]) {
      expect(classifyWireLinkFailure({ code, retry: "later", message: "" }, true)).toBe("network");
    }
  });
});
