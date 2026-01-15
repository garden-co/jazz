import { mkdir } from "node:fs/promises";
import { readFile } from "node:fs/promises";
import { basename, dirname } from "node:path";

import { LocalNode, cojsonInternals } from "cojson";
import { NapiCrypto } from "cojson/crypto/NapiCrypto";
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

  const crypto = await NapiCrypto.create();
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
  indexMap.set("seed:kind", "load-balancing");
  indexMap.set("seed:pdfName", pdfName);
  indexMap.set("seed:items", items);

  const chunkSize = cojsonInternals.TRANSACTION_CONFIG.MAX_RECOMMENDED_TX_SIZE;

  let fileCount = 0;
  let mapCount = 0;

  const toSync = [];

  for (let i = 1; i <= items; i++) {
    if (i % 5 === 0) {
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
      indexMap.set(`file:${fileCount}`, stream.id);
      fileCount++;
      toSync.push(stream);
    } else {
      const map = group.createMap();
      map.set("kind", "map");
      map.set("i", i);
      map.set("value", `v${i}`);

      indexMap.set(`map:${mapCount}`, map.id);
      mapCount++;
      toSync.push(map);
    }

    if (i % 200 === 0) {
      console.log("Generated ", i, " covalues");
    }

    // Assumption, sync happens on microtask, storage is sync so it is enough to wait one timer to ensure that all the toSync values are stored
    if (toSync.length >= 10) {
      await sleep(0);
      for (const value of toSync) {
        value.core.unmount();
      }
      toSync.length = 0;
    }
  }

  indexMap.set("seed:files", fileCount);
  indexMap.set("seed:maps", mapCount);

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
