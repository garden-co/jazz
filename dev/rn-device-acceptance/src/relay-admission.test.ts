import assert from "node:assert/strict";
import test from "node:test";
import { proveAdmittedRelay } from "./relay-admission.ts";

const bytes = (encoded: string) => Uint8Array.from(atob(encoded), (value) => value.charCodeAt(0));
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
    () => execute(btoa(String.fromCharCode(2, 8))),
    /unknown client or relay handle/,
  );
  await assert.rejects(
    () => execute(btoa(String.fromCharCode(4, 8))),
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
