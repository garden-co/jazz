export function generate_nonce(input: Uint8Array): Uint8Array;
export function blake3_hash_once(data: Uint8Array): Uint8Array;
export function blake3_hash_once_with_context(
  data: Uint8Array,
  context: Uint8Array,
): Uint8Array;
