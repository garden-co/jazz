import { statSync } from "node:fs";
import { pathToFileURL } from "node:url";

// Project acceptance budget, not a claimed npmjs service limit. Alpha.54
// failed at 209,615,782 compressed bytes and recovered at 193,492,106.
export const MAX_TARBALL_BYTES = 195_000_000;
export const REQUEST_METADATA_RESERVE_BYTES = 1_000_000;
export const MAX_ESTIMATED_REQUEST_BYTES = 261_000_000;
export function verifyRnPackSize(tarballBytes) {
  if (!Number.isSafeInteger(tarballBytes) || tarballBytes <= 0)
    throw new Error("RN tarball size must be a positive safe integer");
  const base64Bytes = 4 * Math.ceil(tarballBytes / 3);
  const estimatedRequestBytes = base64Bytes + REQUEST_METADATA_RESERVE_BYTES;
  const receipt = {
    tarballBytes,
    base64Bytes,
    base64OverheadBytes: base64Bytes - tarballBytes,
    requestMetadataReserveBytes: REQUEST_METADATA_RESERVE_BYTES,
    estimatedRequestBytes,
    maxTarballBytes: MAX_TARBALL_BYTES,
    maxEstimatedRequestBytes: MAX_ESTIMATED_REQUEST_BYTES,
  };
  if (tarballBytes >= MAX_TARBALL_BYTES || estimatedRequestBytes >= MAX_ESTIMATED_REQUEST_BYTES)
    throw new Error(`RN package exceeds project upload budget: ${JSON.stringify(receipt)}`);
  return receipt;
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  if (process.argv.length !== 3) throw new Error("usage: verify-rn-pack-size.mjs <tarball>");
  const info = statSync(process.argv[2]);
  if (!info.isFile()) throw new Error("RN tarball must be a regular file");
  console.log(JSON.stringify(verifyRnPackSize(info.size)));
}
