import { describe, expect, test } from "vitest";

import { blake3HashOnce, generateNonce, Blake3Hasher } from '../index'; // ensure the package builds correctly

describe("blake3", () => {

  test("hashOnce", () => {
    const inputString = "test input";
    const hash = blake3HashOnce(new TextEncoder().encode(inputString));

    // BLAKE3 produces 32-byte hashes
    expect(hash.length).toBe(32);

    // Same input should produce same hash
    const hash2 = blake3HashOnce(new TextEncoder().encode(inputString));
    expect(Array.from(hash)).toEqual(Array.from(hash2));

    // Different input should produce different hash
    const hash3 = blake3HashOnce(new TextEncoder().encode("different input"));
    expect(Array.from(hash)).not.toEqual(Array.from(hash3));
  })

  test("generateNonce", () => {
    const inputString = "test input";
    const nonce = generateNonce(new TextEncoder().encode(inputString));
    expect(nonce.length).toBe(24);

    // Same input should produce same nonce
    const nonce2 = generateNonce(new TextEncoder().encode(inputString));
    expect(Array.from(nonce)).toEqual(Array.from(nonce2));

    // Different input should produce different nonce
    const nonce3 = generateNonce(new TextEncoder().encode("different input"));
    expect(Array.from(nonce)).not.toEqual(Array.from(nonce3));  
  });

  test("blake3 Hasher State", () => {
    const state = new Blake3Hasher();
    expect(state).toBeDefined();
    expect(state instanceof Object).toBe(true);
  });

  test("incremental hashing", () => {
    const state = new Blake3Hasher();
    
    const data = new Uint8Array([1, 2, 3, 4, 5]);
    state.update(data);

    // Check that this matches a direct hash
    const hash1 = blake3HashOnce(data);
    const stateHash1 = state.finalize();
    expect(Array.from(hash1), 'First update should match direct hash').toEqual(Array.from(stateHash1));

    // Verify the exact expected hash 
    const exptected_firtst_hash = Uint8Array.from([
            2, 79, 103, 192, 66, 90, 61, 192, 47, 186, 245, 140, 185, 61, 229, 19, 46, 61, 117,
            197, 25, 250, 160, 186, 218, 33, 73, 29, 136, 201, 112, 87,
    ]);
    expect(Array.from(stateHash1), 'First update should match expected hash').toEqual(Array.from(exptected_firtst_hash));

    const state2 = new Blake3Hasher();

    // Compare with a single hash of all data
    const data2 = new Uint8Array([6, 7, 8, 9, 10]);
    state2.update(data);
    state2.update(data2);

    const combinedData = new Uint8Array([...data, ...data2]);
    
    // Check that this matches a direct hash
    const hash2 = blake3HashOnce(combinedData);
    const stateHash2 = state2.finalize();
    expect(Array.from(hash2), 'Final state should match direct hash of all data').toEqual(Array.from(stateHash2));

    // Test final hash matches expected value

    const expected_final_hash = Uint8Array.from([
            165, 131, 141, 69, 2, 69, 39, 236, 196, 244, 180, 213, 147, 124, 222, 39, 68, 223, 54,
            176, 242, 97, 200, 101, 204, 79, 21, 233, 56, 51, 1, 199,
    ])
    expect(Array.from(stateHash2), "Final state should match expected hash").toEqual(Array.from(expected_final_hash));
  });
    
});
