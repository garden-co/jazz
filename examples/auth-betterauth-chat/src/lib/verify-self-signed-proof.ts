/**
 * Server-side proof-of-possession verification for self-signed JWTs.
 *
 * Uses the NAPI binding to call the same Rust verification logic
 * that the Jazz server uses. This ensures the proof token:
 * 1. Has iss = "urn:jazz:self-signed"
 * 2. Has alg = EdDSA with a valid Ed25519 signature
 * 3. Has a valid, matching aud claim
 * 4. Has sub == UUIDv5(namespace, jazz_pub_key)
 * 5. Is not expired and within max TTL
 */
import { verifySelfSignedToken } from "jazz-tools/testing";

export function verifySelfSignedProofToken(proofToken: string, expectedAudience: string): string {
  return verifySelfSignedToken(proofToken, expectedAudience);
}
