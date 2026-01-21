import { readFile } from "node:fs/promises";
import { basename } from "node:path";

import {
  cojsonInternals,
  CoValueCore,
  type RawCoID,
  type RawCoList,
} from "cojson";

import type { ParsedArgs } from "../../utils/args.ts";
import { getFlagNumber, getFlagString } from "../../utils/args.ts";
import { SEED_CONFIG_KEYS } from "../../schema.ts";
import {
  initSeedContext,
  finalizeSeedContext,
  syncPendingValues,
  assertNonEmptyString,
  sleep,
} from "../../utils/seedHelpers.ts";
import type { DurationSeedResult } from "./types.ts";

/**
 * Seed the duration scenario: files and maps for concurrent loading.
 */
export async function seed(args: ParsedArgs): Promise<DurationSeedResult> {
  const dbPath = getFlagString(args, "db") ?? "./seed.db";
  const pdfPath = getFlagString(args, "pdf") ?? "./assets/sample.pdf";
  const items = getFlagNumber(args, "items") ?? 100;

  if (!Number.isFinite(items) || items <= 0) {
    throw new Error(
      `Invalid --items "${String(getFlagString(args, "items"))}"`,
    );
  }

  const pdfBuf = await readFile(assertNonEmptyString(pdfPath, "--pdf"));
  const pdfBytes = pdfBuf.byteLength;
  const pdfName = basename(pdfPath);

  const ctx = await initSeedContext(dbPath, "duration");
  const { group, configMap } = ctx;

  const chunkSize = cojsonInternals.TRANSACTION_CONFIG.MAX_RECOMMENDED_TX_SIZE;

  // Create CoLists to store the IDs
  const fileIdsList = group.createList<RawCoList<RawCoID>>();
  const mapIdsList = group.createList<RawCoList<RawCoID>>();

  const fileIds: RawCoID[] = [];
  const mapIds: RawCoID[] = [];
  const toSync: { core: CoValueCore }[] = [];

  for (let i = 1; i <= items; i++) {
    if (i % 2 === 0) {
      // Create file
      const stream = group.createBinaryStream();
      stream.startBinaryStream(
        {
          mimeType: "application/pdf",
          fileName: pdfName,
          totalSizeBytes: pdfBytes,
        },
        "trusting",
      );

      for (let off = 0; off < pdfBuf.length; off += chunkSize) {
        const chunk = pdfBuf.subarray(
          off,
          Math.min(off + chunkSize, pdfBuf.length),
        );
        stream.pushBinaryStreamChunk(chunk);
      }
      stream.endBinaryStream("trusting");
      fileIds.push(stream.id);
      fileIdsList.append(stream.id, undefined, "trusting");
      toSync.push(stream);
    } else {
      // Create map
      const map = group.createMap();
      map.set("kind", "map");
      map.set("i", i);
      map.set("value", `v${i}`);
      mapIds.push(map.id);
      mapIdsList.append(map.id, undefined, "trusting");
      toSync.push(map);
    }

    if (i % 200 === 0) {
      console.log("Generated", i, "covalues");
    }

    await syncPendingValues(toSync);
  }

  // Flush remaining
  if (toSync.length > 0) {
    await sleep(0);
    for (const value of toSync) {
      value.core.unmount();
    }
  }

  // Store CoList references in the config map
  configMap.set(SEED_CONFIG_KEYS.FILE_IDS, fileIdsList.id);
  configMap.set(SEED_CONFIG_KEYS.MAP_IDS, mapIdsList.id);
  configMap.set(SEED_CONFIG_KEYS.PDF_NAME, pdfName);
  configMap.set(SEED_CONFIG_KEYS.PDF_BYTES, pdfBytes);

  const baseResult = await finalizeSeedContext(ctx, "duration");

  const result: DurationSeedResult = {
    ...baseResult,
    scenario: "duration",
    fileIds,
    mapIds,
    pdfName,
    pdfBytes,
  };

  console.log(
    JSON.stringify(
      {
        ...result,
        fileIds: `[${fileIds.length} ids]`,
        mapIds: `[${mapIds.length} ids]`,
      },
      null,
      2,
    ),
  );

  return result;
}
