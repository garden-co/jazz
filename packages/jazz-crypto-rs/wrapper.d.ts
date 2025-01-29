export function generate_nonce(input: Uint8Array): Uint8Array;
export function blake3_hash_once(data: Uint8Array): Uint8Array;
export function blake3_hash_once_with_context(
  data: Uint8Array,
  context: Uint8Array,
): Uint8Array;
export function blake3_empty_state(): Uint8Array;
export function blake3_update_state(
  state: Uint8Array,
  data: Uint8Array,
): Uint8Array;
export function blake3_digest_for_state(state: Uint8Array): Uint8Array;

export class Blake3State {
  constructor();
  update(data: Uint8Array): void;
  finalize(): Uint8Array;
  free(): void;
}

export function blake3_incremental_update(
  state: Blake3State,
  data: Uint8Array,
): Blake3State;
