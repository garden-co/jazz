import { mkdir } from "node:fs/promises";
import { readFile } from "node:fs/promises";
import { basename, dirname } from "node:path";

import { LocalNode, cojsonInternals } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { getBetterSqliteStorage } from "cojson-storage-sqlite";

import type { ParsedArgs } from "./utils/args.ts";
import { getFlagNumber, getFlagString } from "./utils/args.ts";

type SeedResult = {
  db: string;
  items: number;
  files: number;
  maps: number;
  indexMapId: string;
  groupId: string;
  pdfBytes: number;
};

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function assertNonEmptyString(
  value: string | undefined,
  label: string,
): string {
  if (!value || value.trim() === "") {
    throw new Error(`Missing required ${label}`);
  }
  return value;
}

export async function seedDb(args: ParsedArgs): Promise<SeedResult> {
  const dbPath = getFlagString(args, "db") ?? "./seed.db";
  const pdfPath = getFlagString(args, "pdf") ?? "./assets/sample.pdf";
  const items = getFlagNumber(args, "items") ?? 100;
  if (!Number.isFinite(items) || items <= 0) {
    throw new Error(
      `Invalid --items "${String(getFlagString(args, "items"))}"`,
    );
  }

  await mkdir(dirname(dbPath), { recursive: true });

  const pdfBuf = await readFile(assertNonEmptyString(pdfPath, "--pdf"));
  const pdfBytes = pdfBuf.byteLength;
  const pdfName = basename(pdfPath);

  const crypto = await WasmCrypto.create();
  const agentSecret = crypto.newRandomAgentSecret();
  const agentID = crypto.getAgentID(agentSecret);

  const node = new LocalNode(
    agentSecret,
    crypto.newRandomSessionID(agentID),
    crypto,
  );

  const storage = getBetterSqliteStorage(dbPath);
  node.setStorage(storage);

  const group = node.createGroup();
  // Ensure other agents (sync server + workers) can read/write without sharing secrets.
  group.addMember("everyone", "writer");

  const indexMap = group.createMap();
  indexMap.set("seed:kind", "load-balancing", "trusting");
  indexMap.set("seed:pdfName", pdfName, "trusting");
  indexMap.set("seed:items", items, "trusting");

  const chunkSize = cojsonInternals.TRANSACTION_CONFIG.MAX_RECOMMENDED_TX_SIZE;

  let fileCount = 0;
  let mapCount = 0;

  for (let i = 0; i < items; i++) {
    if (i % 2 === 0) {
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
        stream.pushBinaryStreamChunk(chunk, "trusting");
        // Yield to keep Node responsive when `--items` is large.
        if (off === 0 || off + chunkSize >= pdfBuf.length) continue;
        if (off % (chunkSize * 8) === 0) {
          await sleep(0);
        }
      }
      stream.endBinaryStream("trusting");

      indexMap.set(`file:${fileCount}`, stream.id, "trusting");
      fileCount++;
    } else {
      const map = group.createMap();
      map.set("kind", "map", "trusting");
      map.set("i", i, "trusting");
      map.set("value", `v${i}`, "trusting");

      indexMap.set(`map:${mapCount}`, map.id, "trusting");
      mapCount++;
    }
  }

  indexMap.set("seed:files", fileCount, "trusting");
  indexMap.set("seed:maps", mapCount, "trusting");

  // Give storage a moment to settle before shutdown; avoids flaky partial writes.
  await sleep(200);
  node.gracefulShutdown();

  const res: SeedResult = {
    db: dbPath,
    items,
    files: fileCount,
    maps: mapCount,
    indexMapId: indexMap.id,
    groupId: group.id,
    pdfBytes,
  };

  console.log(JSON.stringify(res, null, 2));
  return res;
}
