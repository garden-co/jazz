import assert from "node:assert/strict";
import test from "node:test";
import {
  proveAdmittedRelay,
  proveAuthScopeSwitch,
  proveLogoutRevocation,
} from "./relay-admission.ts";
import { decodeBase64 as bytes, encodeBase64 } from "./base64.ts";

const admitted = Uint8Array.from({ length: 32 }, (_, index) => index + 1);

test("Probe follows a successful Open carrying the exact admitted capability", async () => {
  const commands: Uint8Array[] = [];
  await proveAdmittedRelay(
    {
      async execute(command) {
        const decoded = bytes(command);
        commands.push(decoded);
        if (decoded[0] === 1) return "AQk="; // Opened { relay: 9 }
        if (decoded[0] === 2 && decoded[1] === 9) return "Agc="; // Attached { client: 7 }
        if (decoded[0] === 0) return "AAM="; // Probe { abi: 3 }
        if (decoded[0] === 3 && decoded[1] === 7) return "AwE="; // Closed { true }
        if (decoded[0] === 4 && decoded[1] === 9) return "AwE="; // Closed { true }
        throw new Error("unexpected command");
      },
    },
    admitted,
  );
  assert.deepEqual(
    commands.map((command) => command[0]),
    [1, 2, 0, 3, 4],
  );
  assert.deepEqual(commands[0]!.slice(3), admitted);
});

test("a substituted admitted relay handle is rejected by the strict executor", async () => {
  const execute = async (command: string) => {
    const decoded = bytes(command);
    if (decoded[0] === 1) return "AQk=";
    if (decoded[0] === 2 && decoded[1] === 9) return "Agc=";
    if (decoded[0] === 0) return "AAM=";
    if ((decoded[0] === 3 && decoded[1] === 7) || (decoded[0] === 4 && decoded[1] === 9))
      return "AwE=";
    throw new Error("native relay rejected an unknown client or relay handle");
  };
  await proveAdmittedRelay({ execute }, admitted);
  await assert.rejects(
    () => execute(encodeBase64(Uint8Array.of(2, 8))),
    /unknown client or relay handle/,
  );
  await assert.rejects(
    () => execute(encodeBase64(Uint8Array.of(4, 8))),
    /unknown client or relay handle/,
  );
});

test("a capability that native admission did not install cannot reach Probe", async () => {
  const commands: Uint8Array[] = [];
  await assert.rejects(() =>
    proveAdmittedRelay(
      {
        async execute(command) {
          const decoded = bytes(command);
          commands.push(decoded);
          if (decoded[0] === 1) throw new Error("native relay rejected uninstalled capability");
          return "AAM=";
        },
      },
      new Uint8Array(32),
    ),
  );
  assert.deepEqual(
    commands.map((command) => command[0]),
    [1],
  );
});

test("relay ABI diagnostics identify the failed operation without exposing command data", async () => {
  const diagnostics: string[] = [];
  const capability = Uint8Array.from({ length: 32 }, (_, index) => index);
  await assert.rejects(() =>
    proveAdmittedRelay(
      {
        async execute() {
          throw new Error(`native rejected capability=${encodeBase64(capability)}`);
        },
      },
      capability,
      (stage) => diagnostics.push(stage),
    ),
  );
  assert.deepEqual(diagnostics, ["relay-open-failed"]);
  assert.doesNotMatch(diagnostics.join("\n"), /capability|AAECAw/);
});

test("admission proof preserves the primary failure when cleanup also fails", async () => {
  const diagnostics: string[] = [];
  await assert.rejects(
    () =>
      proveAdmittedRelay(
        {
          async execute(command) {
            const decoded = bytes(command);
            if (decoded[0] === 1) return "AQk=";
            if (decoded[0] === 2) return "Agc=";
            if (decoded[0] === 0) throw new Error("ABI probe failed");
            throw new Error("cleanup failed");
          },
        },
        admitted,
        (stage) => diagnostics.push(stage),
      ),
    /ABI probe failed/,
  );
  assert.deepEqual(diagnostics, ["relay-open-failed", "relay-attach-failed", "relay-probe-failed"]);
});

test("trusted logout removes old capability and aliases before a fresh admission", async () => {
  const original = Uint8Array.from({ length: 32 }, (_, index) => index + 1);
  const replacement = Uint8Array.from({ length: 32 }, (_, index) => 64 - index);
  let revoked = false;
  const executor = {
    async execute(command: string) {
      const decoded = bytes(command);
      if (decoded[0] === 1) {
        if (revoked && decoded.slice(3).every((byte, index) => byte === original[index]))
          throw new Error("revoked capability");
        return revoked ? "AQo=" : "AQk="; // Opened { relay: 9|10 }
      }
      if (decoded[0] === 2) {
        if (revoked && decoded[1] === 9) throw new Error("removed relay alias");
        return revoked ? "Agg=" : "Agc="; // Attached { client: 7|8 }
      }
      if (decoded[0] === 3 && decoded[1] === 7) return revoked ? "AwA=" : "AwE="; // Closed { false|true }
      if (decoded[0] === 3 && decoded[1] === 8) return "AwE=";
      if (decoded[0] === 0) return "AAM="; // Probe { abi: 3 }
      if (decoded[0] === 4 && decoded[1] === 9) return "AwE=";
      if (decoded[0] === 4 && decoded[1] === 10) return "AwE=";
      throw new Error("unexpected command");
    },
  };
  await proveLogoutRevocation(
    { executor, capability: original },
    async () => {
      revoked = true;
    },
    async () => ({ executor, capability: replacement }),
  );
});

test("trusted auth switching rejects scope A before scope B can attach", async () => {
  const scopeA = Uint8Array.from({ length: 32 }, (_, index) => index + 1);
  const scopeB = Uint8Array.from({ length: 32 }, (_, index) => 128 + index);
  let switched = false;
  const executor = {
    async execute(command: string) {
      const decoded = bytes(command);
      if (decoded[0] === 1) {
        if (switched && decoded.slice(3).every((byte, index) => byte === scopeA[index]))
          throw new Error("scope A was revoked");
        return switched ? "AQo=" : "AQk=";
      }
      if (decoded[0] === 2) {
        if (switched && decoded[1] === 9) throw new Error("scope A relay was removed");
        return switched ? "Agg=" : "Agc=";
      }
      if (decoded[0] === 3 && decoded[1] === 7) return switched ? "AwA=" : "AwE=";
      if (decoded[0] === 3 && decoded[1] === 8) return "AwE=";
      if (decoded[0] === 0) return "AAM=";
      if (decoded[0] === 4 && (decoded[1] === 9 || decoded[1] === 10)) return "AwE=";
      throw new Error("unexpected command");
    },
  };
  const replacement = await proveAuthScopeSwitch({ executor, capability: scopeA }, async () => {
    switched = true;
    return { executor, capability: scopeB };
  });
  assert.equal(switched, true);
  assert.deepEqual(replacement.capability, scopeB);
});
