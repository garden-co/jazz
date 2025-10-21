import { describe, bench } from "vitest";
import { faker } from "@faker-js/faker";

import * as cojson from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { NapiCrypto } from "cojson/crypto/NapiCrypto";
import {
  generateRealisticData,
  generateRealisticTasks,
  generateRealisticArticle,
  measureContentSize,
  calculateSavings,
  importCoValue,
  BENCHMARK_CONSTANTS,
} from "./benchmark-utils";

const crypto = await WasmCrypto.create();
const napiCrypto = await NapiCrypto.create();

/**
 * Unified compression benchmarks
 *
 * This file tests compression from multiple angles:
 *
 * 1. WRITE PERFORMANCE - Transaction creation overhead (map.set, list.append, etc.)
 * 2. READ PERFORMANCE - Import/deserialization overhead (loading existing data)
 * 3. BULK OPERATIONS - Many consecutive operations
 * 4. COPLAINTEXT - Text editing scenarios (collaborative, real-time, etc.)
 * 5. EFFECTIVENESS ANALYSIS - Compression ratios and savings
 *
 * All tests use realistic data (users, posts, comments, articles) generated with Faker
 * to better represent real-world usage patterns.
 */

// Different payload sizes to test compression effectiveness
// Note: Compression is now explicit with compress: true parameter
const PAYLOAD_SIZES = {
  SMALL: 512, // Small payload - compression overhead may outweigh benefits
  MODERATE: 1024, // 1KB payload - compression starts to show benefits
  MEDIUM: 4096, // 4KB - good compression ratio
  LARGE: 100 * 1024, // 100KB - maximum recommended tx size
};

function generateFixtures(
  crypto: any,
  payloadSize: number,
  usePrivate: boolean,
  compress: boolean = false,
) {
  const account = cojson.LocalNode.internalCreateAccount({
    crypto,
  });

  const group = account.core.node.createGroup();
  const map = group.createMap();

  // Create realistic data
  const testData = generateRealisticData(payloadSize);

  // Make several updates to create realistic transaction history
  for (let i = 0; i < BENCHMARK_CONSTANTS.NUM_MAP_UPDATES; i++) {
    map.set(
      `key_${i}`,
      JSON.stringify(testData),
      usePrivate ? "private" : "trusting",
      { compress },
    );
  }

  return { map, group };
}

// ============================================================================
// SECTION 1: READ PERFORMANCE - Import/Deserialization
// ============================================================================
// These tests measure the overhead of loading/deserializing existing data
// from storage or network. They test how fast we can import pre-created
// transactions with different compression settings.

describe("Import performance - Small payloads (512B)\n", () => {
  const { map: mapPrivate } = generateFixtures(
    crypto,
    PAYLOAD_SIZES.SMALL,
    true,
    false,
  );
  const { map: mapTrusting } = generateFixtures(
    crypto,
    PAYLOAD_SIZES.SMALL,
    false,
    false,
  );
  const { map: mapCompressed } = generateFixtures(
    crypto,
    PAYLOAD_SIZES.SMALL,
    true,
    true,
  );

  const contentPrivate =
    mapPrivate.core.verified?.newContentSince(undefined) ?? [];
  const contentTrusting =
    mapTrusting.core.verified?.newContentSince(undefined) ?? [];
  const contentCompressed =
    mapCompressed.core.verified?.newContentSince(undefined) ?? [];

  const sizePrivate = measureContentSize(contentPrivate);
  const sizeTrusting = measureContentSize(contentTrusting);
  const sizeCompressed = measureContentSize(contentCompressed);
  const savingsVsPrivate = calculateSavings(sizePrivate, sizeCompressed);
  const savingsVsTrusting = calculateSavings(sizeTrusting, sizeCompressed);

  describe(
    `[Small 512B] Private: ${sizePrivate} bytes, Trusting: ${sizeTrusting} bytes, ` +
      `Compressed: ${sizeCompressed} bytes (${savingsVsPrivate}% vs private, ${savingsVsTrusting}% vs trusting)\n`,
    () => {
      bench(
        "private (encrypted, no compression)",
        () => {
          importCoValue(mapPrivate, contentPrivate);
        },
        { iterations: 200 },
      );

      bench(
        "trusting (no encryption, no compression)",
        () => {
          importCoValue(mapTrusting, contentTrusting);
        },
        { iterations: 200 },
      );

      bench(
        "private with compress:true",
        () => {
          importCoValue(mapCompressed, contentCompressed);
        },
        { iterations: 200 },
      );
    },
  );
});

describe("Import performance - Moderate payloads (1KB)\n", () => {
  const { map: mapPrivate } = generateFixtures(
    crypto,
    PAYLOAD_SIZES.MODERATE,
    true,
    false,
  );
  const { map: mapTrusting } = generateFixtures(
    crypto,
    PAYLOAD_SIZES.MODERATE,
    false,
    false,
  );
  const { map: mapCompressed } = generateFixtures(
    crypto,
    PAYLOAD_SIZES.MODERATE,
    true,
    true,
  );

  const contentPrivate =
    mapPrivate.core.verified?.newContentSince(undefined) ?? [];
  const contentTrusting =
    mapTrusting.core.verified?.newContentSince(undefined) ?? [];
  const contentCompressed =
    mapCompressed.core.verified?.newContentSince(undefined) ?? [];

  const sizePrivate = measureContentSize(contentPrivate);
  const sizeTrusting = measureContentSize(contentTrusting);
  const sizeCompressed = measureContentSize(contentCompressed);
  const savings = calculateSavings(sizePrivate, sizeCompressed);

  describe(
    `[1KB] Private: ${sizePrivate} bytes, Trusting: ${sizeTrusting} bytes, ` +
      `Compressed: ${sizeCompressed} bytes (${savings}% savings)\n`,
    () => {
      bench(
        "private (encrypted, no compression)",
        () => {
          importCoValue(mapPrivate, contentPrivate);
        },
        { iterations: 200 },
      );

      bench(
        "trusting (no encryption, no compression)",
        () => {
          importCoValue(mapTrusting, contentTrusting);
        },
        { iterations: 200 },
      );

      bench(
        "private with compress:true",
        () => {
          importCoValue(mapCompressed, contentCompressed);
        },
        { iterations: 200 },
      );
    },
  );
});

describe("Import performance - Medium payloads (4KB)\n", () => {
  const { map: mapPrivate } = generateFixtures(
    crypto,
    PAYLOAD_SIZES.MEDIUM,
    true,
    false,
  );
  const { map: mapTrusting } = generateFixtures(
    crypto,
    PAYLOAD_SIZES.MEDIUM,
    false,
    false,
  );
  const { map: mapCompressed } = generateFixtures(
    crypto,
    PAYLOAD_SIZES.MEDIUM,
    true,
    true,
  );

  const contentPrivate =
    mapPrivate.core.verified?.newContentSince(undefined) ?? [];
  const contentTrusting =
    mapTrusting.core.verified?.newContentSince(undefined) ?? [];
  const contentCompressed =
    mapCompressed.core.verified?.newContentSince(undefined) ?? [];

  const sizePrivate = measureContentSize(contentPrivate);
  const sizeTrusting = measureContentSize(contentTrusting);
  const sizeCompressed = measureContentSize(contentCompressed);
  const savings = calculateSavings(sizePrivate, sizeCompressed);

  describe(
    `[4KB] Private: ${sizePrivate} bytes, Trusting: ${sizeTrusting} bytes, ` +
      `Compressed: ${sizeCompressed} bytes (${savings}% savings)\n`,
    () => {
      bench(
        "private (encrypted, no compression)",
        () => {
          importCoValue(mapPrivate, contentPrivate);
        },
        { iterations: 200 },
      );

      bench(
        "trusting (no encryption, no compression)",
        () => {
          importCoValue(mapTrusting, contentTrusting);
        },
        { iterations: 200 },
      );

      bench(
        "private with compress:true",
        () => {
          importCoValue(mapCompressed, contentCompressed);
        },
        { iterations: 200 },
      );
    },
  );
});

describe("Import performance - Large payloads (100KB)\n", () => {
  const { map: mapPrivate } = generateFixtures(
    crypto,
    PAYLOAD_SIZES.LARGE,
    true,
    false,
  );
  const { map: mapTrusting } = generateFixtures(
    crypto,
    PAYLOAD_SIZES.LARGE,
    false,
    false,
  );
  const { map: mapCompressed } = generateFixtures(
    crypto,
    PAYLOAD_SIZES.LARGE,
    true,
    true,
  );

  const contentPrivate =
    mapPrivate.core.verified?.newContentSince(undefined) ?? [];
  const contentTrusting =
    mapTrusting.core.verified?.newContentSince(undefined) ?? [];
  const contentCompressed =
    mapCompressed.core.verified?.newContentSince(undefined) ?? [];

  const sizePrivate = measureContentSize(contentPrivate);
  const sizeTrusting = measureContentSize(contentTrusting);
  const sizeCompressed = measureContentSize(contentCompressed);
  const savings = calculateSavings(sizePrivate, sizeCompressed);

  describe(
    `[100KB] Private: ${sizePrivate} bytes, Trusting: ${sizeTrusting} bytes, ` +
      `Compressed: ${sizeCompressed} bytes (${savings}% savings)\n`,
    () => {
      bench(
        "private (encrypted, no compression)",
        () => {
          importCoValue(mapPrivate, contentPrivate);
        },
        { iterations: 100 },
      );

      bench(
        "trusting (no encryption, no compression)",
        () => {
          importCoValue(mapTrusting, contentTrusting);
        },
        { iterations: 100 },
      );

      bench(
        "private with compress:true",
        () => {
          importCoValue(mapCompressed, contentCompressed);
        },
        { iterations: 100 },
      );
    },
  );
});

describe("Import performance with NAPI - Medium payloads (4KB)\n", () => {
  const { map: mapPrivateNapi } = generateFixtures(
    napiCrypto,
    PAYLOAD_SIZES.MEDIUM,
    true,
    false,
  );
  const { map: mapTrustingNapi } = generateFixtures(
    napiCrypto,
    PAYLOAD_SIZES.MEDIUM,
    false,
    false,
  );
  const { map: mapCompressedNapi } = generateFixtures(
    napiCrypto,
    PAYLOAD_SIZES.MEDIUM,
    true,
    true,
  );

  const contentPrivate =
    mapPrivateNapi.core.verified?.newContentSince(undefined) ?? [];
  const contentTrusting =
    mapTrustingNapi.core.verified?.newContentSince(undefined) ?? [];
  const contentCompressed =
    mapCompressedNapi.core.verified?.newContentSince(undefined) ?? [];

  bench(
    "NAPI - private (encrypted, no compression)",
    () => {
      importCoValue(mapPrivateNapi, contentPrivate);
    },
    { iterations: 200 },
  );

  bench(
    "NAPI - trusting (no encryption, no compression)",
    () => {
      importCoValue(mapTrustingNapi, contentTrusting);
    },
    { iterations: 200 },
  );

  bench(
    "NAPI - private with compress:true",
    () => {
      importCoValue(mapCompressedNapi, contentCompressed);
    },
    { iterations: 200 },
  );
});

// ============================================================================
// SECTION 2: WRITE PERFORMANCE - Transaction Creation
// ============================================================================
// These tests measure the overhead of CREATING new transactions on-the-fly.
// They test how fast we can write new data with different compression settings.

describe("Write performance - Transaction creation (4KB payload)\n", () => {
  const { map: mapPrivate } = generateFixtures(
    crypto,
    PAYLOAD_SIZES.MEDIUM,
    true,
    false,
  );
  const { map: mapTrusting } = generateFixtures(
    crypto,
    PAYLOAD_SIZES.MEDIUM,
    false,
    false,
  );
  const { map: mapCompressed } = generateFixtures(
    crypto,
    PAYLOAD_SIZES.MEDIUM,
    true,
    false,
  );

  const realisticPayload = JSON.stringify(
    generateRealisticData(PAYLOAD_SIZES.MEDIUM),
  );

  bench(
    "create private transaction (no compression)",
    () => {
      mapPrivate.set(`key_${Math.random()}`, realisticPayload, "private");
    },
    { iterations: 500 },
  );

  bench(
    "create trusting transaction (no encryption, no compression)",
    () => {
      mapTrusting.set(`key_${Math.random()}`, realisticPayload, "trusting");
    },
    { iterations: 500 },
  );

  bench(
    "create private transaction with compress:true",
    () => {
      mapCompressed.set(`key_${Math.random()}`, realisticPayload, "private", {
        compress: true,
      });
    },
    { iterations: 500 },
  );
});

describe("Write performance - Small payload (~900 bytes)\n", () => {
  const payload = JSON.stringify(generateRealisticData(900));

  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const mapPrivate = groupPrivate.createMap();

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const mapTrusting = groupTrusting.createMap();

  const accountCompressed = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupCompressed = accountCompressed.core.node.createGroup();
  const mapCompressed = groupCompressed.createMap();

  describe(`Payload size: ${payload.length} bytes (realistic user data)\n`, () => {
    bench(
      "write - private (encrypted, no compression)",
      () => {
        mapPrivate.set(`key_${Math.random()}`, payload, "private");
      },
      { iterations: 1000 },
    );

    bench(
      "write - trusting (no encryption, no compression)",
      () => {
        mapTrusting.set(`key_${Math.random()}`, payload, "trusting");
      },
      { iterations: 1000 },
    );

    bench(
      "write - private with compress:true",
      () => {
        mapCompressed.set(`key_${Math.random()}`, payload, "private", {
          compress: true,
        });
      },
      { iterations: 1000 },
    );
  });
});

describe("Write performance - Large payload (~5KB)\n", () => {
  const payload = JSON.stringify(generateRealisticData(5000));

  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const mapPrivate = groupPrivate.createMap();

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const mapTrusting = groupTrusting.createMap();

  const accountCompressed = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupCompressed = accountCompressed.core.node.createGroup();
  const mapCompressed = groupCompressed.createMap();

  describe(`Payload size: ${payload.length} bytes (realistic user/post/comment data)\n`, () => {
    bench(
      "write - private (encrypted, no compression)",
      () => {
        mapPrivate.set(`key_${Math.random()}`, payload, "private");
      },
      { iterations: 1000 },
    );

    bench(
      "write - trusting (no encryption, no compression)",
      () => {
        mapTrusting.set(`key_${Math.random()}`, payload, "trusting");
      },
      { iterations: 1000 },
    );

    bench(
      "write - private with compress:true",
      () => {
        mapCompressed.set(`key_${Math.random()}`, payload, "private", {
          compress: true,
        });
      },
      { iterations: 1000 },
    );
  });
});

// ============================================================================
// SECTION 3: BULK OPERATIONS
// ============================================================================
// These tests measure performance when doing many consecutive operations,
// which is common in sync scenarios or batch updates.

describe("Bulk operations - Many small updates (100x)\n", () => {
  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const mapPrivate = groupPrivate.createMap();

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const mapTrusting = groupTrusting.createMap();

  const accountCompressed = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupCompressed = accountCompressed.core.node.createGroup();
  const mapCompressed = groupCompressed.createMap();

  // Small payloads - realistic task data
  const smallPayload = JSON.stringify(generateRealisticTasks(1)[0]);

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

  bench(
    "100 small updates - private with compress:true",
    () => {
      for (let i = 0; i < 100; i++) {
        mapCompressed.set(`key_${i}`, smallPayload, "private", {
          compress: true,
        });
      }
    },
    { iterations: 50 },
  );
});

describe("Bulk operations - Many large updates (50x, ~5KB each)\n", () => {
  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const mapPrivate = groupPrivate.createMap();

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const mapTrusting = groupTrusting.createMap();

  const accountCompressed = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupCompressed = accountCompressed.core.node.createGroup();
  const mapCompressed = groupCompressed.createMap();

  // Large payloads - realistic user/post/comment data
  const largePayload = JSON.stringify(generateRealisticData(5000));

  bench(
    "50 large updates - private (encrypted, no compression)",
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

  bench(
    "50 large updates - private with compress:true",
    () => {
      for (let i = 0; i < 50; i++) {
        mapCompressed.set(`key_${i}`, largePayload, "private", {
          compress: true,
        });
      }
    },
    { iterations: 20 },
  );
});

// ============================================================================
// SECTION 4: LIST & MAP OPERATIONS
// ============================================================================

describe("List operations with compression - comparing data types\n", () => {
  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const listPrivate = groupPrivate.createList();

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const listTrusting = groupTrusting.createList();

  const accountCompressed = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupCompressed = accountCompressed.core.node.createGroup();
  const listCompressed = groupCompressed.createList();

  // Initialize with realistic items (task/todo items using Faker)
  faker.seed(456);
  for (let i = 0; i < 10; i++) {
    const task = {
      id: faker.string.uuid(),
      title: faker.hacker.phrase(),
      description: faker.lorem.sentence(),
      status: faker.helpers.arrayElement([
        "todo",
        "in-progress",
        "done",
        "blocked",
      ]),
      priority: faker.helpers.arrayElement(["low", "medium", "high", "urgent"]),
      assignee: faker.person.fullName(),
      tags: faker.helpers.arrayElements(
        ["bug", "feature", "improvement", "documentation"],
        { min: 0, max: 3 },
      ),
      createdAt: faker.date.past({ years: 1 }).toISOString(),
      dueDate: faker.date.future({ years: 2 }).toISOString(),
      estimatedHours: faker.number.int({ min: 1, max: 40 }),
      completed: faker.datatype.boolean(),
    };
    listPrivate.append(JSON.stringify(task), undefined, "private");
    listTrusting.append(JSON.stringify(task), undefined, "trusting");
    listCompressed.append(JSON.stringify(task), undefined, "private", {
      compress: true,
    });
  }

  const contentPrivate =
    listPrivate.core.verified?.newContentSince(undefined) ?? [];
  const contentTrusting =
    listTrusting.core.verified?.newContentSince(undefined) ?? [];
  const contentCompressed =
    listCompressed.core.verified?.newContentSince(undefined) ?? [];

  const sizePrivate = measureContentSize(contentPrivate);
  const sizeTrusting = measureContentSize(contentTrusting);
  const sizeCompressed = measureContentSize(contentCompressed);
  const savings = calculateSavings(sizePrivate, sizeCompressed);

  describe(
    `[List] Private: ${sizePrivate} bytes, Trusting: ${sizeTrusting} bytes, ` +
      `Compressed: ${sizeCompressed} bytes (${savings}% savings)\n`,
    () => {
      bench(
        "list import - private (encrypted, no compression)",
        () => {
          importCoValue(listPrivate, contentPrivate);
        },
        { iterations: 200 },
      );

      bench(
        "list import - trusting (no encryption, no compression)",
        () => {
          importCoValue(listTrusting, contentTrusting);
        },
        { iterations: 200 },
      );

      bench(
        "list import - private with compress:true",
        () => {
          importCoValue(listCompressed, contentCompressed);
        },
        { iterations: 200 },
      );
    },
  );
});

// ============================================================================
// SECTION 5: COPLAINTEXT OPERATIONS
// ============================================================================
// These tests focus on text editing scenarios with compression.
// CoPlainText has built-in compression for private mode on text > 1KB.

describe("CoPlainText - Text operations with compression\n", () => {
  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  // Use realistic text content
  const initialText = generateRealisticArticle(10);
  const textPrivate = groupPrivate.createPlainText(initialText);

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const textTrusting = groupTrusting.createPlainText(initialText);

  // Make realistic edits (simulating actual text editing)
  faker.seed(BENCHMARK_CONSTANTS.DETERMINISTIC_SEED);
  const editWords = faker.helpers.arrayElements(
    [
      "however",
      "furthermore",
      "therefore",
      "meanwhile",
      "additionally",
      "consequently",
      "nevertheless",
    ],
    {
      min: BENCHMARK_CONSTANTS.NUM_TEXT_EDITS,
      max: BENCHMARK_CONSTANTS.NUM_TEXT_EDITS,
    },
  );
  for (let i = 0; i < BENCHMARK_CONSTANTS.NUM_TEXT_EDITS; i++) {
    const word = editWords[i];
    textPrivate.insertAfter(i * 10, ` ${word}`, "private");
    textTrusting.insertAfter(i * 10, ` ${word}`, "trusting");
  }

  const contentPrivate =
    textPrivate.core.verified?.newContentSince(undefined) ?? [];
  const contentTrusting =
    textTrusting.core.verified?.newContentSince(undefined) ?? [];

  const sizePrivate = measureContentSize(contentPrivate);
  const sizeTrusting = measureContentSize(contentTrusting);
  const savings = calculateSavings(sizeTrusting, sizePrivate);

  describe(`[Text] Private: ${sizePrivate} bytes (${savings}% savings vs ${sizeTrusting} trusting bytes)\n`, () => {
    bench(
      "text import - private (encrypted + auto-compressed)",
      () => {
        importCoValue(textPrivate, contentPrivate);
      },
      { iterations: 200 },
    );

    bench(
      "text import - trusting (no overhead)",
      () => {
        importCoValue(textTrusting, contentTrusting);
      },
      { iterations: 200 },
    );
  });
});

describe("CoPlainText - Long document (~10KB)\n", () => {
  // Use realistic article text
  const longText = generateRealisticArticle(25); // ~10KB of realistic article text

  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const textPrivate = groupPrivate.createPlainText(longText, null, "private");

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const textTrusting = groupTrusting.createPlainText(
    longText,
    null,
    "trusting",
  );

  const contentPrivate =
    textPrivate.core.verified?.newContentSince(undefined) ?? [];
  const contentTrusting =
    textTrusting.core.verified?.newContentSince(undefined) ?? [];

  const sizePrivate = measureContentSize(contentPrivate);
  const sizeTrusting = measureContentSize(contentTrusting);
  const ratio = calculateSavings(sizeTrusting, sizePrivate);

  describe(`[CoPlainText 10KB] Private: ${sizePrivate} bytes, Trusting: ${sizeTrusting} bytes, Savings: ${ratio}%\n`, () => {
    bench(
      "import long document - private (encrypted + auto-compressed)",
      () => {
        importCoValue(textPrivate, contentPrivate);
      },
      { iterations: 200 },
    );

    bench(
      "import long document - trusting (no overhead)",
      () => {
        importCoValue(textTrusting, contentTrusting);
      },
      { iterations: 200 },
    );
  });
});

describe("CoPlainText - Collaborative editing (many small edits)\n", () => {
  const initialText = generateRealisticArticle(3); // Start with realistic article text

  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const textPrivate = groupPrivate.createPlainText(
    initialText,
    null,
    "private",
  );

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const textTrusting = groupTrusting.createPlainText(
    initialText,
    null,
    "trusting",
  );

  // Simulate collaborative editing: many small insertions
  faker.seed(BENCHMARK_CONSTANTS.DETERMINISTIC_SEED);
  for (let i = 0; i < BENCHMARK_CONSTANTS.NUM_COLLABORATIVE_EDITS; i++) {
    const pos = Math.min(i * 10, textPrivate.entries().length);
    const char = faker.helpers.arrayElement([" ", ".", ",", "!", "?"]);
    textPrivate.insertAfter(pos, char, "private");
    textTrusting.insertAfter(pos, char, "trusting");
  }

  const contentPrivate =
    textPrivate.core.verified?.newContentSince(undefined) ?? [];
  const contentTrusting =
    textTrusting.core.verified?.newContentSince(undefined) ?? [];

  const sizePrivate = measureContentSize(contentPrivate);
  const sizeTrusting = measureContentSize(contentTrusting);
  const savings = calculateSavings(sizeTrusting, sizePrivate);

  describe(`[CoPlainText many edits] Private: ${sizePrivate} bytes (${savings}% savings vs ${sizeTrusting} trusting bytes)\n`, () => {
    bench(
      "import with many edits - private (encrypted + auto-compressed)",
      () => {
        importCoValue(textPrivate, contentPrivate);
      },
      { iterations: 200 },
    );

    bench(
      "import with many edits - trusting (no overhead)",
      () => {
        importCoValue(textTrusting, contentTrusting);
      },
      { iterations: 200 },
    );
  });
});

describe("CoPlainText - Real-time editing performance\n", () => {
  const initialText = generateRealisticArticle(5); // Realistic article for editing

  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const textPrivate = groupPrivate.createPlainText(initialText);

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const textTrusting = groupTrusting.createPlainText(initialText);

  bench(
    "insert single char - private (encrypted + auto-compressed)",
    () => {
      const pos = textPrivate.entries().length;
      textPrivate.insertAfter(pos, "a", "private");
    },
    { iterations: 1000 },
  );

  bench(
    "insert single char - trusting (no overhead)",
    () => {
      const pos = textTrusting.entries().length;
      textTrusting.insertAfter(pos, "a", "trusting");
    },
    { iterations: 1000 },
  );

  bench(
    "insert word - private (encrypted + auto-compressed)",
    () => {
      const pos = textPrivate.entries().length;
      textPrivate.insertAfter(pos, " hello", "private");
    },
    { iterations: 1000 },
  );

  bench(
    "insert word - trusting (no overhead)",
    () => {
      const pos = textTrusting.entries().length;
      textTrusting.insertAfter(pos, " hello", "trusting");
    },
    { iterations: 1000 },
  );
});

describe("CoPlainText - Large document with NAPI (~15KB)\n", () => {
  // Test with NAPI crypto for better performance
  const largeArticle = generateRealisticArticle(35); // ~15KB of realistic article text

  const accountPrivate = cojson.LocalNode.internalCreateAccount({
    crypto: napiCrypto,
  });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const textPrivate = groupPrivate.createPlainText(
    largeArticle,
    null,
    "private",
  );

  const accountTrusting = cojson.LocalNode.internalCreateAccount({
    crypto: napiCrypto,
  });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const textTrusting = groupTrusting.createPlainText(
    largeArticle,
    null,
    "trusting",
  );

  // Add some edits to simulate realistic document evolution
  for (let i = 0; i < BENCHMARK_CONSTANTS.NUM_LARGE_DOC_EDITS; i++) {
    const pos = Math.floor(
      (textPrivate.entries().length * i) /
        BENCHMARK_CONSTANTS.NUM_LARGE_DOC_EDITS,
    );
    textPrivate.insertAfter(pos, " [edited]", "private");
    textTrusting.insertAfter(pos, " [edited]", "trusting");
  }

  const contentPrivate =
    textPrivate.core.verified?.newContentSince(undefined) ?? [];
  const contentTrusting =
    textTrusting.core.verified?.newContentSince(undefined) ?? [];

  const sizePrivate = measureContentSize(contentPrivate);
  const sizeTrusting = measureContentSize(contentTrusting);
  const ratio = calculateSavings(sizeTrusting, sizePrivate);

  describe(`[CoPlainText NAPI 15KB] Private: ${sizePrivate} bytes, Trusting: ${sizeTrusting} bytes, Savings: ${ratio}%\n`, () => {
    bench(
      "NAPI: import large document - private (encrypted + auto-compressed)",
      () => {
        importCoValue(textPrivate, contentPrivate);
      },
      { iterations: 200 },
    );

    bench(
      "NAPI: import large document - trusting (no overhead)",
      () => {
        importCoValue(textTrusting, contentTrusting);
      },
      { iterations: 200 },
    );
  });
});

describe("CoPlainText - Single character insertions (real-time typing)\n", () => {
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

  describe("CoPlainText single char: simulating real-time typing\n", () => {
    bench(
      "type single char - private (encryption overhead only, too small to compress)",
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
});

describe("CoPlainText - Batch insertions with NAPI", () => {
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

// ============================================================================
// SECTION 6: COMPRESSION EFFECTIVENESS ANALYSIS
// ============================================================================
// These tests analyze compression ratios and effectiveness across different
// data sizes and types, providing insights into when compression is beneficial.

describe("Compression effectiveness - Realistic data at different sizes\n", () => {
  function measureCompression(size: number) {
    const payload = JSON.stringify(generateRealisticData(size));

    const account = cojson.LocalNode.internalCreateAccount({ crypto });
    const group = account.core.node.createGroup();
    const map = group.createMap();

    for (let i = 0; i < 5; i++) {
      map.set(`key_${i}`, payload, "private", { compress: true });
    }

    const content = map.core.verified?.newContentSince(undefined) ?? [];
    const contentSize = measureContentSize(content);

    return { originalSize: payload.length * 5, contentSize };
  }

  const results = {
    "1KB": measureCompression(1000),
    "5KB": measureCompression(5000),
    "10KB": measureCompression(10000),
  };

  const summaryLines = [
    "=== Compression Effectiveness (realistic data, 5 updates each, compress:true) ===",
    ...Object.entries(results).map(([size, { originalSize, contentSize }]) => {
      const ratio = calculateSavings(originalSize, contentSize);
      return `${size.padEnd(12)}: Original ${originalSize} bytes -> Compressed ${contentSize} bytes (${ratio}% savings)`;
    }),
  ].join("\n");

  describe(`${summaryLines}\n`, () => {
    bench("baseline - measure only", () => {
      // This is just to show the results above
    });
  });
});
