import { describe, expect, it, vi } from "vitest";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";

const schema = {};
const node = new Uint8Array(16);
const author = new TextEncoder().encode(JSON.stringify(["https://issuer.example", "alice"]));
const proof = {
  token: "signed-proof",
  appId: "proof-app",
  claimedAuthor: JSON.stringify(["urn:jazz:local-first", "alice"]),
};

function fakeDb() {
  return { setTickScheduler: vi.fn() };
}

describe("self-signed native open ABI", () => {
  it("fails explicitly against an old native artifact instead of falling back to its raw open", () => {
    const oldArtifact = { openMemory: vi.fn(() => fakeDb()) };

    expect(
      () =>
        new NativeRuntimeAdapter(oldArtifact, schema, node, author, 1, false, {
          selfSignedClientProof: proof,
        }),
    ).toThrow(/does not support self-signed client opens/);
    expect(oldArtifact.openMemory).not.toHaveBeenCalled();
  });

  it("uses only the distinct proof-bearing entrypoint with the bounded proof fields", () => {
    const openMemory = vi.fn(() => fakeDb());
    const openMemoryWithSelfSignedProof = vi.fn(() => fakeDb());
    const runtime = new NativeRuntimeAdapter(
      { openMemory, openMemoryWithSelfSignedProof },
      schema,
      node,
      author,
      1,
      false,
      { selfSignedClientProof: proof },
    );

    expect(openMemory).not.toHaveBeenCalled();
    expect(openMemoryWithSelfSignedProof).toHaveBeenCalledWith(
      expect.any(Uint8Array),
      expect.any(Uint8Array),
      proof.token,
      proof.appId,
      proof.claimedAuthor,
    );
    void runtime.close();
  });
});
