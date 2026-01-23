import type { CoValueCore, RawCoID, RawCoList } from "cojson";

import type { ParsedArgs } from "../../utils/args.ts";
import { getFlagNumber, getFlagString } from "../../utils/args.ts";
import { SEED_CONFIG_KEYS } from "../../schema.ts";
import {
  initSeedContext,
  finalizeSeedContext,
  syncPendingValues,
  sleep,
} from "../../utils/seedHelpers.ts";
import { randomSizeInRange, generateSizedPayload } from "../../utils/rng.ts";
import type { BatchSeedResult } from "./types.ts";

/**
 * Seed the batch scenario: maps with varying sizes.
 */
export async function seed(args: ParsedArgs): Promise<BatchSeedResult> {
  const dbPath = getFlagString(args, "db") ?? "./seed.db";
  const mapCount = getFlagNumber(args, "maps") ?? 100;
  const minSize = getFlagNumber(args, "minSize") ?? 100;
  const maxSize = getFlagNumber(args, "maxSize") ?? 1024;

  if (!Number.isFinite(mapCount) || mapCount <= 0) {
    throw new Error(`Invalid --maps "${String(getFlagString(args, "maps"))}"`);
  }
  if (minSize > maxSize) {
    throw new Error("--minSize must be <= --maxSize");
  }

  const ctx = await initSeedContext(dbPath, "batch");
  const { group, configMap } = ctx;

  // Create CoList to store the IDs
  const mapIdsList = group.createList<RawCoList<RawCoID>>();

  const mapIds: RawCoID[] = [];
  const toSync: { core: CoValueCore }[] = [];
  const baseSeed = Date.now();

  for (let i = 0; i < mapCount; i++) {
    const seed = baseSeed ^ (i * 2654435761);
    const size = randomSizeInRange(minSize, maxSize, seed);
    const payload = generateSizedPayload(size, seed + 1);

    const map = group.createMap();
    map.set("kind", "sized-map");
    map.set("index", i);
    map.set("size", size);
    map.set("payload", payload);

    mapIds.push(map.id);
    mapIdsList.append(map.id, undefined, "trusting");
    toSync.push(map);

    if (i % 200 === 0 && i > 0) {
      console.log("Generated", i, "maps");
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

  // Store CoList reference in the config map
  configMap.set(SEED_CONFIG_KEYS.MAP_IDS, mapIdsList.id);
  configMap.set(SEED_CONFIG_KEYS.MIN_SIZE, minSize);
  configMap.set(SEED_CONFIG_KEYS.MAX_SIZE, maxSize);

  const baseResult = await finalizeSeedContext(ctx, "batch");

  const result: BatchSeedResult = {
    ...baseResult,
    scenario: "batch",
    mapIds,
    minSize,
    maxSize,
  };

  console.log(
    JSON.stringify(
      {
        ...result,
        mapIds: `[${mapIds.length} ids]`,
      },
      null,
      2,
    ),
  );

  return result;
}
