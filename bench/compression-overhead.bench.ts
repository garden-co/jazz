import { describe, bench } from "vitest";

import * as cojson from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { NapiCrypto } from "cojson/crypto/NapiCrypto";

const crypto = await WasmCrypto.create();
const napiCrypto = await NapiCrypto.create();

/**
 * This benchmark focuses on measuring the pure overhead of compression
 * by comparing identical operations with private (compressed) vs trusting (uncompressed) modes
 */

// Compression threshold is 1024 bytes
const COMPRESSION_THRESHOLD = 1024;

function generateDataPattern(
  size: number,
  pattern: "repetitive" | "random" | "json",
) {
  switch (pattern) {
    case "repetitive":
      // Highly compressible - same character repeated
      return "A".repeat(size);
    case "random":
      // Poorly compressible - random characters
      return Array.from({ length: size }, () =>
        String.fromCharCode(Math.floor(Math.random() * 26) + 65),
      ).join("");
    case "json":
      // Realistic JSON data - moderately compressible
      const items = [];
      const itemSize = Math.max(1, Math.floor(size / 200));
      for (let i = 0; i < itemSize; i++) {
        items.push({
          id: `item_${i}`,
          name: `Name ${i}`,
          description: `Description for item ${i}`,
          value: Math.random() * 1000,
          tags: ["tag1", "tag2", "tag3"],
        });
      }
      return JSON.stringify(items);
  }
}

describe("Overhead: Just below compression threshold (900 bytes)", () => {
  const payload = generateDataPattern(900, "repetitive");

  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const mapPrivate = groupPrivate.createMap();

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const mapTrusting = groupTrusting.createMap();

  console.log(
    `Payload size: ${payload.length} bytes (below ${COMPRESSION_THRESHOLD})`,
  );

  bench(
    "private mode (encrypted, no compression)",
    () => {
      mapPrivate.set(`key_${Math.random()}`, payload, "private");
    },
    { iterations: 1000 },
  );

  bench(
    "trusting mode (no encryption, no compression)",
    () => {
      mapTrusting.set(`key_${Math.random()}`, payload, "trusting");
    },
    { iterations: 1000 },
  );
});

describe("Overhead: Just above compression threshold (1200 bytes, repetitive)", () => {
  const payload = generateDataPattern(1200, "repetitive");

  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const mapPrivate = groupPrivate.createMap();

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const mapTrusting = groupTrusting.createMap();

  console.log(
    `Payload size: ${payload.length} bytes (above ${COMPRESSION_THRESHOLD}, highly compressible)`,
  );

  bench(
    "private mode (encrypted + compressed)",
    () => {
      mapPrivate.set(`key_${Math.random()}`, payload, "private");
    },
    { iterations: 1000 },
  );

  bench(
    "trusting mode (no encryption, no compression)",
    () => {
      mapTrusting.set(`key_${Math.random()}`, payload, "trusting");
    },
    { iterations: 1000 },
  );
});

describe("Overhead: Random data (poorly compressible, 2KB)", () => {
  const payload = generateDataPattern(2048, "random");

  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const mapPrivate = groupPrivate.createMap();

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const mapTrusting = groupTrusting.createMap();

  console.log(`Payload size: ${payload.length} bytes (poorly compressible)`);

  bench(
    "private mode (encrypted + compressed, poor ratio)",
    () => {
      mapPrivate.set(`key_${Math.random()}`, payload, "private");
    },
    { iterations: 1000 },
  );

  bench(
    "trusting mode (no encryption, no compression)",
    () => {
      mapTrusting.set(`key_${Math.random()}`, payload, "trusting");
    },
    { iterations: 1000 },
  );
});

describe("Overhead: Realistic JSON (moderately compressible, 5KB)", () => {
  const payload = generateDataPattern(5000, "json");

  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const mapPrivate = groupPrivate.createMap();

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const mapTrusting = groupTrusting.createMap();

  console.log(`Payload size: ${payload.length} bytes (realistic JSON)`);

  bench(
    "private mode (encrypted + compressed)",
    () => {
      mapPrivate.set(`key_${Math.random()}`, payload, "private");
    },
    { iterations: 1000 },
  );

  bench(
    "trusting mode (no encryption, no compression)",
    () => {
      mapTrusting.set(`key_${Math.random()}`, payload, "trusting");
    },
    { iterations: 1000 },
  );
});

describe("Overhead: Large highly compressible data (50KB)", () => {
  const payload = generateDataPattern(50 * 1024, "repetitive");

  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const mapPrivate = groupPrivate.createMap();

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const mapTrusting = groupTrusting.createMap();

  console.log(`Payload size: ${payload.length} bytes (highly compressible)`);

  bench(
    "private mode (encrypted + compressed)",
    () => {
      mapPrivate.set(`key_${Math.random()}`, payload, "private");
    },
    { iterations: 200 },
  );

  bench(
    "trusting mode (no encryption, no compression)",
    () => {
      mapTrusting.set(`key_${Math.random()}`, payload, "trusting");
    },
    { iterations: 200 },
  );
});

describe("Deserialization overhead: Medium JSON (5KB)", () => {
  function setupFixture(usePrivate: boolean) {
    const account = cojson.LocalNode.internalCreateAccount({ crypto });
    const group = account.core.node.createGroup();
    const map = group.createMap();

    const payload = generateDataPattern(5000, "json");
    for (let i = 0; i < 20; i++) {
      map.set(`key_${i}`, payload, usePrivate ? "private" : "trusting");
    }

    return {
      map,
      content: map.core.verified?.newContentSince(undefined) ?? [],
    };
  }

  const { map: mapPrivate, content: contentPrivate } = setupFixture(true);
  const { map: mapTrusting, content: contentTrusting } = setupFixture(false);

  const sizePrivate = new TextEncoder().encode(
    JSON.stringify(contentPrivate),
  ).length;
  const sizeTrusting = new TextEncoder().encode(
    JSON.stringify(contentTrusting),
  ).length;
  const savings = ((1 - sizePrivate / sizeTrusting) * 100).toFixed(1);

  console.log(
    `Content size - Private: ${sizePrivate} bytes, Trusting: ${sizeTrusting} bytes, Savings: ${savings}%`,
  );

  function deserialize(map: any, content: any) {
    map.core.node.getCoValue(map.id).unmount();
    for (const msg of content) {
      map.core.node.syncManager.handleNewContent(msg, "storage");
    }
    const coValue = map.core.node.getCoValue(map.id);
    coValue.getCurrentContent();
  }

  bench(
    "deserialize private (decrypt + decompress)",
    () => {
      deserialize(mapPrivate, contentPrivate);
    },
    { iterations: 300 },
  );

  bench(
    "deserialize trusting (no overhead)",
    () => {
      deserialize(mapTrusting, contentTrusting);
    },
    { iterations: 300 },
  );
});

describe("Deserialization overhead with NAPI: Medium JSON (5KB)", () => {
  function setupFixture(usePrivate: boolean) {
    const account = cojson.LocalNode.internalCreateAccount({
      crypto: napiCrypto,
    });
    const group = account.core.node.createGroup();
    const map = group.createMap();

    const payload = generateDataPattern(5000, "json");
    for (let i = 0; i < 20; i++) {
      map.set(`key_${i}`, payload, usePrivate ? "private" : "trusting");
    }

    return {
      map,
      content: map.core.verified?.newContentSince(undefined) ?? [],
    };
  }

  const { map: mapPrivate, content: contentPrivate } = setupFixture(true);
  const { map: mapTrusting, content: contentTrusting } = setupFixture(false);

  function deserialize(map: any, content: any) {
    map.core.node.getCoValue(map.id).unmount();
    for (const msg of content) {
      map.core.node.syncManager.handleNewContent(msg, "storage");
    }
    const coValue = map.core.node.getCoValue(map.id);
    coValue.getCurrentContent();
  }

  bench(
    "NAPI: deserialize private (decrypt + decompress)",
    () => {
      deserialize(mapPrivate, contentPrivate);
    },
    { iterations: 300 },
  );

  bench(
    "NAPI: deserialize trusting (no overhead)",
    () => {
      deserialize(mapTrusting, contentTrusting);
    },
    { iterations: 300 },
  );
});

describe("Bulk operations overhead: Many small updates", () => {
  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const mapPrivate = groupPrivate.createMap();

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const mapTrusting = groupTrusting.createMap();

  // Small payloads below compression threshold
  const smallPayload = "Small data: " + "X".repeat(100);

  bench(
    "100 small updates - private (encrypted, no compression)",
    () => {
      for (let i = 0; i < 100; i++) {
        mapPrivate.set(`key_${i}`, smallPayload, "private");
      }
    },
    { iterations: 50 },
  );

  bench(
    "100 small updates - trusting (no overhead)",
    () => {
      for (let i = 0; i < 100; i++) {
        mapTrusting.set(`key_${i}`, smallPayload, "trusting");
      }
    },
    { iterations: 50 },
  );
});

describe("Bulk operations overhead: Many large updates", () => {
  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const mapPrivate = groupPrivate.createMap();

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const mapTrusting = groupTrusting.createMap();

  // Large payloads that will be compressed
  const largePayload = generateDataPattern(5000, "json");

  bench(
    "50 large updates - private (encrypted + compressed)",
    () => {
      for (let i = 0; i < 50; i++) {
        mapPrivate.set(`key_${i}`, largePayload, "private");
      }
    },
    { iterations: 20 },
  );

  bench(
    "50 large updates - trusting (no overhead)",
    () => {
      for (let i = 0; i < 50; i++) {
        mapTrusting.set(`key_${i}`, largePayload, "trusting");
      }
    },
    { iterations: 20 },
  );
});

describe("Compression effectiveness by data type", () => {
  function measureCompression(
    size: number,
    pattern: "repetitive" | "random" | "json",
  ) {
    const payload = generateDataPattern(size, pattern);

    const account = cojson.LocalNode.internalCreateAccount({ crypto });
    const group = account.core.node.createGroup();
    const map = group.createMap();

    for (let i = 0; i < 5; i++) {
      map.set(`key_${i}`, payload, "private");
    }

    const content = map.core.verified?.newContentSince(undefined) ?? [];
    const contentSize = new TextEncoder().encode(
      JSON.stringify(content),
    ).length;

    return { originalSize: payload.length * 5, contentSize };
  }

  const results = {
    repetitive: measureCompression(5000, "repetitive"),
    random: measureCompression(5000, "random"),
    json: measureCompression(5000, "json"),
  };

  console.log(
    "\n=== Compression Effectiveness (5KB payload, 5 updates each) ===",
  );
  for (const [type, { originalSize, contentSize }] of Object.entries(results)) {
    const ratio = ((1 - contentSize / originalSize) * 100).toFixed(1);
    console.log(
      `${type.padEnd(12)}: Original ${originalSize} bytes -> Compressed ${contentSize} bytes (${ratio}% savings)`,
    );
  }

  bench("baseline - measure only", () => {
    // This is just to show the results above
  });
});

describe("CoPlainText: Short text below threshold", () => {
  const shortText = "Hello, this is a short text document."; // ~38 bytes

  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const textPrivate = groupPrivate.createPlainText(shortText, null, "private");

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const textTrusting = groupTrusting.createPlainText(
    shortText,
    null,
    "trusting",
  );

  console.log(`CoPlainText short: ${shortText.length} bytes (below threshold)`);

  bench(
    "short text - private (no compression expected)",
    () => {
      textPrivate.insertAfter(
        textPrivate.entries().length,
        " More text.",
        "private",
      );
    },
    { iterations: 1000 },
  );

  bench(
    "short text - trusting (no overhead)",
    () => {
      textTrusting.insertAfter(
        textTrusting.entries().length,
        " More text.",
        "trusting",
      );
    },
    { iterations: 1000 },
  );
});

describe("CoPlainText: At compression threshold (1KB)", () => {
  const thresholdText = "A".repeat(1024);

  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const textPrivate = groupPrivate.createPlainText(
    thresholdText,
    null,
    "private",
  );

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const textTrusting = groupTrusting.createPlainText(
    thresholdText,
    null,
    "trusting",
  );

  const contentPrivate =
    textPrivate.core.verified?.newContentSince(undefined) ?? [];
  const contentTrusting =
    textTrusting.core.verified?.newContentSince(undefined) ?? [];

  const sizePrivate = new TextEncoder().encode(
    JSON.stringify(contentPrivate),
  ).length;
  const sizeTrusting = new TextEncoder().encode(
    JSON.stringify(contentTrusting),
  ).length;

  console.log(
    `CoPlainText at threshold: Original ${thresholdText.length} bytes, Private ${sizePrivate} bytes, Trusting ${sizeTrusting} bytes`,
  );

  bench(
    "threshold text - private (compressed)",
    () => {
      textPrivate.insertAfter(textPrivate.entries().length, "X", "private");
    },
    { iterations: 1000 },
  );

  bench(
    "threshold text - trusting (no compression)",
    () => {
      textTrusting.insertAfter(textTrusting.entries().length, "X", "trusting");
    },
    { iterations: 1000 },
  );
});

describe("CoPlainText: Large repetitive text (highly compressible)", () => {
  const repetitiveText = "The quick brown fox jumps over the lazy dog. ".repeat(
    100,
  ); // ~4.4KB

  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const textPrivate = groupPrivate.createPlainText(
    repetitiveText,
    null,
    "private",
  );

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const textTrusting = groupTrusting.createPlainText(
    repetitiveText,
    null,
    "trusting",
  );

  const contentPrivate =
    textPrivate.core.verified?.newContentSince(undefined) ?? [];
  const contentTrusting =
    textTrusting.core.verified?.newContentSince(undefined) ?? [];

  const sizePrivate = new TextEncoder().encode(
    JSON.stringify(contentPrivate),
  ).length;
  const sizeTrusting = new TextEncoder().encode(
    JSON.stringify(contentTrusting),
  ).length;
  const savings = ((1 - sizePrivate / sizeTrusting) * 100).toFixed(1);

  console.log(
    `CoPlainText repetitive: Private ${sizePrivate} bytes, Trusting ${sizeTrusting} bytes, Savings ${savings}%`,
  );

  bench(
    "repetitive text update - private (compressed)",
    () => {
      const pos = textPrivate.entries().length;
      textPrivate.insertAfter(pos, " Added text.", "private");
    },
    { iterations: 1000 },
  );

  bench(
    "repetitive text update - trusting (uncompressed)",
    () => {
      const pos = textTrusting.entries().length;
      textTrusting.insertAfter(pos, " Added text.", "trusting");
    },
    { iterations: 1000 },
  );
});

describe("CoPlainText: Realistic article (moderately compressible)", () => {
  const article = `
# Introduction to Collaborative Text Editing

Collaborative text editing has revolutionized the way teams work together on documents.
Unlike traditional methods where only one person can edit at a time, modern collaborative
editors allow multiple users to work simultaneously without conflicts.

## Key Features

1. Real-time synchronization
2. Conflict-free replicated data types (CRDTs)
3. Offline support with automatic merging
4. Fine-grained access control

## Technical Implementation

The underlying technology uses advanced algorithms to ensure that edits from different
users can be merged automatically. This is achieved through operational transformation
or CRDT-based approaches.

`.repeat(10); // ~5KB

  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const textPrivate = groupPrivate.createPlainText(article, null, "private");

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const textTrusting = groupTrusting.createPlainText(article, null, "trusting");

  const contentPrivate =
    textPrivate.core.verified?.newContentSince(undefined) ?? [];
  const contentTrusting =
    textTrusting.core.verified?.newContentSince(undefined) ?? [];

  const sizePrivate = new TextEncoder().encode(
    JSON.stringify(contentPrivate),
  ).length;
  const sizeTrusting = new TextEncoder().encode(
    JSON.stringify(contentTrusting),
  ).length;
  const savings = ((1 - sizePrivate / sizeTrusting) * 100).toFixed(1);

  console.log(
    `CoPlainText article: Private ${sizePrivate} bytes, Trusting ${sizeTrusting} bytes, Savings ${savings}%`,
  );

  function loadText(text: any, content: any) {
    text.core.node.getCoValue(text.id).unmount();
    for (const msg of content) {
      text.core.node.syncManager.handleNewContent(msg, "storage");
    }
    const coValue = text.core.node.getCoValue(text.id);
    coValue.getCurrentContent();
  }

  bench(
    "load article - private (decrypt + decompress)",
    () => {
      loadText(textPrivate, contentPrivate);
    },
    { iterations: 300 },
  );

  bench(
    "load article - trusting (no overhead)",
    () => {
      loadText(textTrusting, contentTrusting);
    },
    { iterations: 300 },
  );
});

describe("CoPlainText: Single character insertions (real-time typing)", () => {
  const baseText = "Hello world";

  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const textPrivate = groupPrivate.createPlainText(baseText, null, "private");

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const textTrusting = groupTrusting.createPlainText(
    baseText,
    null,
    "trusting",
  );

  console.log("CoPlainText single char: simulating real-time typing");

  bench(
    "type single char - private (encryption overhead only)",
    () => {
      const pos = textPrivate.entries().length;
      textPrivate.insertAfter(pos, "a", "private");
    },
    { iterations: 2000 },
  );

  bench(
    "type single char - trusting (baseline)",
    () => {
      const pos = textTrusting.entries().length;
      textTrusting.insertAfter(pos, "a", "trusting");
    },
    { iterations: 2000 },
  );
});

describe("CoPlainText: Batch insertions with NAPI", () => {
  const baseText = "Document start. ";

  const accountPrivate = cojson.LocalNode.internalCreateAccount({
    crypto: napiCrypto,
  });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const textPrivate = groupPrivate.createPlainText(baseText, null, "private");

  const accountTrusting = cojson.LocalNode.internalCreateAccount({
    crypto: napiCrypto,
  });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const textTrusting = groupTrusting.createPlainText(
    baseText,
    null,
    "trusting",
  );

  const paragraph = "This is a new paragraph with meaningful content. ".repeat(
    5,
  );

  bench(
    "NAPI: insert paragraph - private",
    () => {
      const pos = textPrivate.entries().length;
      textPrivate.insertAfter(pos, paragraph, "private");
    },
    { iterations: 500 },
  );

  bench(
    "NAPI: insert paragraph - trusting",
    () => {
      const pos = textTrusting.entries().length;
      textTrusting.insertAfter(pos, paragraph, "trusting");
    },
    { iterations: 500 },
  );
});

describe("CoPlainText: Delete vs Insert overhead", () => {
  function setupText(crypto: any, usePrivate: boolean) {
    const account = cojson.LocalNode.internalCreateAccount({ crypto });
    const group = account.core.node.createGroup();
    return group.createPlainText(
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ".repeat(20), // ~520 bytes
      null,
      usePrivate ? "private" : "trusting",
    );
  }

  const textPrivateInsert = setupText(crypto, true);
  const textTrustingInsert = setupText(crypto, false);
  const textPrivateDelete = setupText(crypto, true);
  const textTrustingDelete = setupText(crypto, false);

  bench(
    "insert - private",
    () => {
      const pos = textPrivateInsert.entries().length;
      textPrivateInsert.insertAfter(pos, "X", "private");
    },
    { iterations: 1000 },
  );

  bench(
    "insert - trusting",
    () => {
      const pos = textTrustingInsert.entries().length;
      textTrustingInsert.insertAfter(pos, "X", "trusting");
    },
    { iterations: 1000 },
  );

  bench(
    "delete - private",
    () => {
      const len = textPrivateDelete.entries().length;
      if (len > 1) {
        textPrivateDelete.deleteRange({ from: len - 1, to: len }, "private");
      }
    },
    { iterations: 1000 },
  );

  bench(
    "delete - trusting",
    () => {
      const len = textTrustingDelete.entries().length;
      if (len > 1) {
        textTrustingDelete.deleteRange({ from: len - 1, to: len }, "trusting");
      }
    },
    { iterations: 1000 },
  );
});
