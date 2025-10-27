import { describe, bench } from "vitest";
import { faker } from "@faker-js/faker";

import * as cojson from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { NapiCrypto } from "cojson/crypto/NapiCrypto";
import {
  generateRealisticTasks,
  generateRealisticData,
  generateRealisticArticle,
  measureContentSize,
  calculateSavings,
  BENCHMARK_CONSTANTS,
} from "./benchmark-utils";

const crypto = await WasmCrypto.create();
const napiCrypto = await NapiCrypto.create();

/**
 * CoList & CoPlainText Pack/Unpack Benchmarks
 *
 * This file benchmarks the packChanges/unpackChanges operations
 * that optimize storage of sequential list operations.
 *
 * When multiple "app" operations are applied consecutively with the same "after"
 * reference, they can be packed into a more compact format:
 * - Unpacked: [{op:"app", value:1, after:"start"}, {op:"app", value:2, after:"start"}, ...]
 * - Packed: [{op:"app", value:1, after:"start", compacted:true}, 2, 3, ...]
 *
 * This benchmark measures:
 * 1. PACK VS NO PACK - Direct comparison with pack enabled/disabled
 * 2. SIZE COMPARISON - Transaction size differences between pack on/off
 * 3. PERFORMANCE COMPARISON - Speed differences with pack on/off
 * 4. DIFFERENT DATA SIZES - Small vs large items with both modes
 * 5. COPLAINTEXT COMPARISON - Text editing with pack on/off
 * 6. NAPI CRYPTO COMPARISON - Performance with native crypto
 * 7. SUMMARY ANALYSIS - Comprehensive comparison across all scenarios
 */

// ============================================================================
// SECTION 1: PACK VS NO PACK - Direct Size & Performance Comparison
// ============================================================================

describe("CoList - Pack ON vs Pack OFF comparison\n", () => {
  const account = cojson.LocalNode.internalCreateAccount({ crypto });
  const group = account.core.node.createGroup();

  // Generate realistic task items
  const tasks = generateRealisticTasks(100);

  describe("10 items - Pack comparison\n", () => {
    const taskStrings10 = tasks.slice(0, 10).map((t) => JSON.stringify(t));

    // Measure with pack enabled
    const listPack = group.createList();
    listPack.appendItems(taskStrings10, undefined, "private", {
      disablePacking: false,
    });
    const contentPack =
      listPack.core.verified?.newContentSince(undefined) ?? [];
    const packedSize = measureContentSize(contentPack);

    // Measure with pack disabled
    const listNoPack = group.createList();
    listNoPack.appendItems(taskStrings10, undefined, "private", {
      disablePacking: true,
    });
    const contentNoPack =
      listNoPack.core.verified?.newContentSince(undefined) ?? [];
    const noPackSize = measureContentSize(contentNoPack);

    const savings = calculateSavings(noPackSize, packedSize);

    describe(`Pack ON: ${packedSize}b | Pack OFF: ${noPackSize}b | Savings: ${savings}%\n`, () => {
      bench(
        "appendItems with pack=true",
        () => {
          const tempList = group.createList();
          tempList.appendItems(taskStrings10, undefined, "private", {
            disablePacking: false,
          });
        },
        { iterations: 1000 },
      );

      bench(
        "appendItems with pack=false",
        () => {
          const tempList = group.createList();
          tempList.appendItems(taskStrings10, undefined, "private", {
            disablePacking: true,
          });
        },
        { iterations: 1000 },
      );

      bench(
        "import with pack=true content",
        () => {
          listPack.core.node.getCoValue(listPack.id).unmount();
          for (const msg of contentPack) {
            listPack.core.node.syncManager.handleNewContent(msg, "storage");
          }
          const cv = listPack.core.node.getCoValue(listPack.id);
          cv.getCurrentContent();
        },
        { iterations: 1000 },
      );

      bench(
        "import with pack=false content",
        () => {
          listNoPack.core.node.getCoValue(listNoPack.id).unmount();
          for (const msg of contentNoPack) {
            listNoPack.core.node.syncManager.handleNewContent(msg, "storage");
          }
          const cv = listNoPack.core.node.getCoValue(listNoPack.id);
          cv.getCurrentContent();
        },
        { iterations: 1000 },
      );
    });
  });

  describe("50 items - Pack comparison\n", () => {
    const taskStrings50 = tasks.slice(0, 50).map((t) => JSON.stringify(t));

    // Measure with pack enabled
    const listPack = group.createList();
    listPack.appendItems(taskStrings50, undefined, "private", {
      disablePacking: false,
    });
    const contentPack =
      listPack.core.verified?.newContentSince(undefined) ?? [];
    const packedSize = measureContentSize(contentPack);

    // Measure with pack disabled
    const listNoPack = group.createList();
    listNoPack.appendItems(taskStrings50, undefined, "private", {
      disablePacking: true,
    });
    const contentNoPack =
      listNoPack.core.verified?.newContentSince(undefined) ?? [];
    const noPackSize = measureContentSize(contentNoPack);

    const savings = calculateSavings(noPackSize, packedSize);

    describe(`Pack ON: ${packedSize}b | Pack OFF: ${noPackSize}b | Savings: ${savings}%\n`, () => {
      bench(
        "appendItems with pack=true",
        () => {
          const tempList = group.createList();
          tempList.appendItems(taskStrings50, undefined, "private", {
            disablePacking: false,
          });
        },
        { iterations: 500 },
      );

      bench(
        "appendItems with pack=false",
        () => {
          const tempList = group.createList();
          tempList.appendItems(taskStrings50, undefined, "private", {
            disablePacking: true,
          });
        },
        { iterations: 500 },
      );

      bench(
        "import with pack=true content",
        () => {
          listPack.core.node.getCoValue(listPack.id).unmount();
          for (const msg of contentPack) {
            listPack.core.node.syncManager.handleNewContent(msg, "storage");
          }
          const cv = listPack.core.node.getCoValue(listPack.id);
          cv.getCurrentContent();
        },
        { iterations: 500 },
      );

      bench(
        "import with pack=false content",
        () => {
          listNoPack.core.node.getCoValue(listNoPack.id).unmount();
          for (const msg of contentNoPack) {
            listNoPack.core.node.syncManager.handleNewContent(msg, "storage");
          }
          const cv = listNoPack.core.node.getCoValue(listNoPack.id);
          cv.getCurrentContent();
        },
        { iterations: 500 },
      );
    });
  });

  describe("100 items - Pack comparison\n", () => {
    const taskStrings100 = tasks.slice(0, 100).map((t) => JSON.stringify(t));

    // Measure with pack enabled
    const listPack = group.createList();
    listPack.appendItems(taskStrings100, undefined, "private", {
      disablePacking: false,
    });
    const contentPack =
      listPack.core.verified?.newContentSince(undefined) ?? [];
    const packedSize = measureContentSize(contentPack);

    // Measure with pack disabled
    const listNoPack = group.createList();
    listNoPack.appendItems(taskStrings100, undefined, "private", {
      disablePacking: true,
    });
    const contentNoPack =
      listNoPack.core.verified?.newContentSince(undefined) ?? [];
    const noPackSize = measureContentSize(contentNoPack);

    const savings = calculateSavings(noPackSize, packedSize);

    describe(`Pack ON: ${packedSize}b | Pack OFF: ${noPackSize}b | Savings: ${savings}%\n`, () => {
      bench(
        "appendItems with pack=true",
        () => {
          const tempList = group.createList();
          tempList.appendItems(taskStrings100, undefined, "private", {
            disablePacking: false,
          });
        },
        { iterations: 200 },
      );

      bench(
        "appendItems with pack=false",
        () => {
          const tempList = group.createList();
          tempList.appendItems(taskStrings100, undefined, "private", {
            disablePacking: true,
          });
        },
        { iterations: 200 },
      );

      bench(
        "import with pack=true content",
        () => {
          listPack.core.node.getCoValue(listPack.id).unmount();
          for (const msg of contentPack) {
            listPack.core.node.syncManager.handleNewContent(msg, "storage");
          }
          const cv = listPack.core.node.getCoValue(listPack.id);
          cv.getCurrentContent();
        },
        { iterations: 200 },
      );

      bench(
        "import with pack=false content",
        () => {
          listNoPack.core.node.getCoValue(listNoPack.id).unmount();
          for (const msg of contentNoPack) {
            listNoPack.core.node.syncManager.handleNewContent(msg, "storage");
          }
          const cv = listNoPack.core.node.getCoValue(listNoPack.id);
          cv.getCurrentContent();
        },
        { iterations: 200 },
      );
    });
  });
});

// ============================================================================
// SECTION 2: SIZE COMPARISON - Different Item Sizes
// ============================================================================

describe("CoList - Pack comparison with different item sizes\n", () => {
  const account = cojson.LocalNode.internalCreateAccount({ crypto });
  const group = account.core.node.createGroup();

  describe("Small items (~100 bytes each, 50 items)\n", () => {
    faker.seed(123);
    const smallItems = Array.from({ length: 50 }, () => faker.lorem.sentence());

    // Pack enabled
    const listPack = group.createList();
    listPack.appendItems(smallItems, undefined, "private", {
      disablePacking: false,
    });
    const contentPack =
      listPack.core.verified?.newContentSince(undefined) ?? [];
    const packedSize = measureContentSize(contentPack);

    // Pack disabled
    const listNoPack = group.createList();
    listNoPack.appendItems(smallItems, undefined, "private", {
      disablePacking: true,
    });
    const contentNoPack =
      listNoPack.core.verified?.newContentSince(undefined) ?? [];
    const noPackSize = measureContentSize(contentNoPack);

    const savings = calculateSavings(noPackSize, packedSize);

    describe(`Pack ON: ${packedSize}b | Pack OFF: ${noPackSize}b | Savings: ${savings}%\n`, () => {
      bench(
        "small items with pack=true",
        () => {
          const tempList = group.createList();
          tempList.appendItems(smallItems, undefined, "private", {
            disablePacking: false,
          });
        },
        { iterations: 500 },
      );

      bench(
        "small items with pack=false",
        () => {
          const tempList = group.createList();
          tempList.appendItems(smallItems, undefined, "private", {
            disablePacking: true,
          });
        },
        { iterations: 500 },
      );

      bench(
        "import small items (pack=true)",
        () => {
          listPack.core.node.getCoValue(listPack.id).unmount();
          for (const msg of contentPack) {
            listPack.core.node.syncManager.handleNewContent(msg, "storage");
          }
          const cv = listPack.core.node.getCoValue(listPack.id);
          cv.getCurrentContent();
        },
        { iterations: 500 },
      );

      bench(
        "import small items (pack=false)",
        () => {
          listNoPack.core.node.getCoValue(listNoPack.id).unmount();
          for (const msg of contentNoPack) {
            listNoPack.core.node.syncManager.handleNewContent(msg, "storage");
          }
          const cv = listNoPack.core.node.getCoValue(listNoPack.id);
          cv.getCurrentContent();
        },
        { iterations: 500 },
      );
    });
  });

  describe("Large items (~1KB each, 50 items)\n", () => {
    const largeItems = Array.from({ length: 50 }, () =>
      JSON.stringify(generateRealisticData(1000)),
    );

    // Pack enabled
    const listPack = group.createList();
    listPack.appendItems(largeItems, undefined, "private", {
      disablePacking: false,
    });
    const contentPack =
      listPack.core.verified?.newContentSince(undefined) ?? [];
    const packedSize = measureContentSize(contentPack);

    // Pack disabled
    const listNoPack = group.createList();
    listNoPack.appendItems(largeItems, undefined, "private", {
      disablePacking: true,
    });
    const contentNoPack =
      listNoPack.core.verified?.newContentSince(undefined) ?? [];
    const noPackSize = measureContentSize(contentNoPack);

    const savings = calculateSavings(noPackSize, packedSize);

    describe(`Pack ON: ${packedSize}b | Pack OFF: ${noPackSize}b | Savings: ${savings}%\n`, () => {
      bench(
        "large items with pack=true",
        () => {
          const tempList = group.createList();
          tempList.appendItems(largeItems, undefined, "private", {
            disablePacking: false,
          });
        },
        { iterations: 200 },
      );

      bench(
        "large items with pack=false",
        () => {
          const tempList = group.createList();
          tempList.appendItems(largeItems, undefined, "private", {
            disablePacking: true,
          });
        },
        { iterations: 200 },
      );

      bench(
        "import large items (pack=true)",
        () => {
          listPack.core.node.getCoValue(listPack.id).unmount();
          for (const msg of contentPack) {
            listPack.core.node.syncManager.handleNewContent(msg, "storage");
          }
          const cv = listPack.core.node.getCoValue(listPack.id);
          cv.getCurrentContent();
        },
        { iterations: 200 },
      );

      bench(
        "import large items (pack=false)",
        () => {
          listNoPack.core.node.getCoValue(listNoPack.id).unmount();
          for (const msg of contentNoPack) {
            listNoPack.core.node.syncManager.handleNewContent(msg, "storage");
          }
          const cv = listNoPack.core.node.getCoValue(listNoPack.id);
          cv.getCurrentContent();
        },
        { iterations: 200 },
      );
    });
  });
});

// ============================================================================
// SECTION 3: PERFORMANCE COMPARISON - Different batch sizes
// ============================================================================

describe("CoList - Performance scaling with pack ON vs OFF\n", () => {
  const account = cojson.LocalNode.internalCreateAccount({ crypto });
  const group = account.core.node.createGroup();

  const tasks = generateRealisticTasks(200);
  const batchSizes = [25, 50, 100, 200];

  for (const size of batchSizes) {
    const taskStrings = tasks.slice(0, size).map((t) => JSON.stringify(t));

    describe(`Batch size: ${size} items\n`, () => {
      // Measure sizes
      const listPack = group.createList();
      listPack.appendItems(taskStrings, undefined, "private", {
        disablePacking: false,
      });
      const contentPack =
        listPack.core.verified?.newContentSince(undefined) ?? [];
      const packedSize = measureContentSize(contentPack);

      const listNoPack = group.createList();
      listNoPack.appendItems(taskStrings, undefined, "private", {
        disablePacking: true,
      });
      const contentNoPack =
        listNoPack.core.verified?.newContentSince(undefined) ?? [];
      const noPackSize = measureContentSize(contentNoPack);

      const savings = calculateSavings(noPackSize, packedSize);
      const iterations = size <= 50 ? 200 : size <= 100 ? 100 : 50;

      describe(`Pack ON: ${packedSize}b | Pack OFF: ${noPackSize}b | Savings: ${savings}%\n`, () => {
        bench(
          "create with pack=true",
          () => {
            const tempList = group.createList();
            tempList.appendItems(taskStrings, undefined, "private", {
              disablePacking: false,
            });
          },
          { iterations },
        );

        bench(
          "create with pack=false",
          () => {
            const tempList = group.createList();
            tempList.appendItems(taskStrings, undefined, "private", {
              disablePacking: true,
            });
          },
          { iterations },
        );
      });
    });
  }
});

// ============================================================================
// SECTION 4: NAPI CRYPTO COMPARISON - Pack ON vs OFF
// ============================================================================

describe("CoList - NAPI Crypto with pack ON vs OFF\n", () => {
  const tasks = generateRealisticTasks(100);
  const taskStrings = tasks.map((t) => JSON.stringify(t));

  const accountNapi = cojson.LocalNode.internalCreateAccount({
    crypto: napiCrypto,
  });
  const groupNapi = accountNapi.core.node.createGroup();

  describe("NAPI: 100 items comparison\n", () => {
    // Measure sizes
    const listPack = groupNapi.createList();
    listPack.appendItems(taskStrings, undefined, "private", {
      disablePacking: false,
    });
    const contentPack =
      listPack.core.verified?.newContentSince(undefined) ?? [];
    const packedSize = measureContentSize(contentPack);

    const listNoPack = groupNapi.createList();
    listNoPack.appendItems(taskStrings, undefined, "private", {
      disablePacking: true,
    });
    const contentNoPack =
      listNoPack.core.verified?.newContentSince(undefined) ?? [];
    const noPackSize = measureContentSize(contentNoPack);

    const savings = calculateSavings(noPackSize, packedSize);

    describe(`NAPI - Pack ON: ${packedSize}b | Pack OFF: ${noPackSize}b | Savings: ${savings}%\n`, () => {
      bench(
        "NAPI: append with pack=true",
        () => {
          const list = groupNapi.createList();
          list.appendItems(taskStrings, undefined, "private", {
            disablePacking: false,
          });
        },
        { iterations: 100 },
      );

      bench(
        "NAPI: append with pack=false",
        () => {
          const list = groupNapi.createList();
          list.appendItems(taskStrings, undefined, "private", {
            disablePacking: true,
          });
        },
        { iterations: 100 },
      );

      bench(
        "NAPI: import with pack=true content",
        () => {
          listPack.core.node.getCoValue(listPack.id).unmount();
          for (const msg of contentPack) {
            listPack.core.node.syncManager.handleNewContent(msg, "storage");
          }
          const cv = listPack.core.node.getCoValue(listPack.id);
          cv.getCurrentContent();
        },
        { iterations: 100 },
      );

      bench(
        "NAPI: import with pack=false content",
        () => {
          listNoPack.core.node.getCoValue(listNoPack.id).unmount();
          for (const msg of contentNoPack) {
            listNoPack.core.node.syncManager.handleNewContent(msg, "storage");
          }
          const cv = listNoPack.core.node.getCoValue(listNoPack.id);
          cv.getCurrentContent();
        },
        { iterations: 100 },
      );
    });
  });
});

// ============================================================================
// SECTION 5: COPLAINTEXT - Pack ON vs OFF comparison
// ============================================================================

describe("CoPlainText - Pack ON vs Pack OFF comparison\n", () => {
  const account = cojson.LocalNode.internalCreateAccount({ crypto });
  const group = account.core.node.createGroup();

  describe("50 characters insertion\n", () => {
    faker.seed(456);
    const sentence = "The quick brown fox jumps over the lazy dog again";

    // Pack enabled
    const textPack = group.createPlainText("", null, "private");
    textPack.insertAfter(-1, sentence, "private", { disablePacking: false });
    const contentPack =
      textPack.core.verified?.newContentSince(undefined) ?? [];
    const packedSize = measureContentSize(contentPack);

    // Pack disabled
    const textNoPack = group.createPlainText("", null, "private");
    textNoPack.insertAfter(-1, sentence, "private", { disablePacking: true });
    const contentNoPack =
      textNoPack.core.verified?.newContentSince(undefined) ?? [];
    const noPackSize = measureContentSize(contentNoPack);

    const savings = calculateSavings(noPackSize, packedSize);

    describe(`Pack ON: ${packedSize}b | Pack OFF: ${noPackSize}b | Savings: ${savings}%\n`, () => {
      bench(
        "insertAfter with pack=true",
        () => {
          const tempText = group.createPlainText("", null, "private");
          tempText.insertAfter(-1, sentence, "private", {
            disablePacking: false,
          });
        },
        { iterations: 1000 },
      );

      bench(
        "insertAfter with pack=false",
        () => {
          const tempText = group.createPlainText("", null, "private");
          tempText.insertAfter(-1, sentence, "private", {
            disablePacking: true,
          });
        },
        { iterations: 1000 },
      );

      bench(
        "import with pack=true content",
        () => {
          textPack.core.node.getCoValue(textPack.id).unmount();
          for (const msg of contentPack) {
            textPack.core.node.syncManager.handleNewContent(msg, "storage");
          }
          const cv = textPack.core.node.getCoValue(textPack.id);
          cv.getCurrentContent();
        },
        { iterations: 1000 },
      );

      bench(
        "import with pack=false content",
        () => {
          textNoPack.core.node.getCoValue(textNoPack.id).unmount();
          for (const msg of contentNoPack) {
            textNoPack.core.node.syncManager.handleNewContent(msg, "storage");
          }
          const cv = textNoPack.core.node.getCoValue(textNoPack.id);
          cv.getCurrentContent();
        },
        { iterations: 1000 },
      );
    });
  });

  describe("500 characters paragraph\n", () => {
    const article = generateRealisticArticle(5);
    const paragraph = article.slice(0, 500);

    // Pack enabled
    const textPack = group.createPlainText("", null, "private");
    textPack.insertAfter(-1, paragraph, "private", { disablePacking: false });
    const contentPack =
      textPack.core.verified?.newContentSince(undefined) ?? [];
    const packedSize = measureContentSize(contentPack);

    // Pack disabled
    const textNoPack = group.createPlainText("", null, "private");
    textNoPack.insertAfter(-1, paragraph, "private", { disablePacking: true });
    const contentNoPack =
      textNoPack.core.verified?.newContentSince(undefined) ?? [];
    const noPackSize = measureContentSize(contentNoPack);

    const savings = calculateSavings(noPackSize, packedSize);

    describe(`Pack ON: ${packedSize}b | Pack OFF: ${noPackSize}b | Savings: ${savings}%\n`, () => {
      bench(
        "insert 500 chars with pack=true",
        () => {
          const tempText = group.createPlainText("", null, "private");
          tempText.insertAfter(-1, paragraph, "private", {
            disablePacking: false,
          });
        },
        { iterations: 500 },
      );

      bench(
        "insert 500 chars with pack=false",
        () => {
          const tempText = group.createPlainText("", null, "private");
          tempText.insertAfter(-1, paragraph, "private", {
            disablePacking: true,
          });
        },
        { iterations: 500 },
      );

      bench(
        "import with pack=true content",
        () => {
          textPack.core.node.getCoValue(textPack.id).unmount();
          for (const msg of contentPack) {
            textPack.core.node.syncManager.handleNewContent(msg, "storage");
          }
          const cv = textPack.core.node.getCoValue(textPack.id);
          cv.getCurrentContent();
        },
        { iterations: 500 },
      );

      bench(
        "import with pack=false content",
        () => {
          textNoPack.core.node.getCoValue(textNoPack.id).unmount();
          for (const msg of contentNoPack) {
            textNoPack.core.node.syncManager.handleNewContent(msg, "storage");
          }
          const cv = textNoPack.core.node.getCoValue(textNoPack.id);
          cv.getCurrentContent();
        },
        { iterations: 500 },
      );
    });
  });

  describe("Large article (~2000 characters)\n", () => {
    const largeArticle = generateRealisticArticle(10).slice(0, 2000);

    // Pack enabled
    const textPack = group.createPlainText("", null, "private");
    textPack.insertAfter(-1, largeArticle, "private", {
      disablePacking: false,
    });
    const contentPack =
      textPack.core.verified?.newContentSince(undefined) ?? [];
    const packedSize = measureContentSize(contentPack);

    // Pack disabled
    const textNoPack = group.createPlainText("", null, "private");
    textNoPack.insertAfter(-1, largeArticle, "private", {
      disablePacking: true,
    });
    const contentNoPack =
      textNoPack.core.verified?.newContentSince(undefined) ?? [];
    const noPackSize = measureContentSize(contentNoPack);

    const savings = calculateSavings(noPackSize, packedSize);

    describe(`Pack ON: ${packedSize}b | Pack OFF: ${noPackSize}b | Savings: ${savings}%\n`, () => {
      bench(
        "insert 2000 chars with pack=true",
        () => {
          const tempText = group.createPlainText("", null, "private");
          tempText.insertAfter(-1, largeArticle, "private", {
            disablePacking: false,
          });
        },
        { iterations: 200 },
      );

      bench(
        "insert 2000 chars with pack=false",
        () => {
          const tempText = group.createPlainText("", null, "private");
          tempText.insertAfter(-1, largeArticle, "private", {
            disablePacking: true,
          });
        },
        { iterations: 200 },
      );

      bench(
        "import with pack=true content",
        () => {
          textPack.core.node.getCoValue(textPack.id).unmount();
          for (const msg of contentPack) {
            textPack.core.node.syncManager.handleNewContent(msg, "storage");
          }
          const cv = textPack.core.node.getCoValue(textPack.id);
          cv.getCurrentContent();
        },
        { iterations: 200 },
      );

      bench(
        "import with pack=false content",
        () => {
          textNoPack.core.node.getCoValue(textNoPack.id).unmount();
          for (const msg of contentNoPack) {
            textNoPack.core.node.syncManager.handleNewContent(msg, "storage");
          }
          const cv = textNoPack.core.node.getCoValue(textNoPack.id);
          cv.getCurrentContent();
        },
        { iterations: 200 },
      );
    });
  });
});

// ============================================================================
// SECTION 6: SUMMARY ANALYSIS - Pack ON vs OFF across all scenarios
// ============================================================================

describe("Summary - Pack ON vs Pack OFF space savings\n", () => {
  const account = cojson.LocalNode.internalCreateAccount({ crypto });
  const group = account.core.node.createGroup();

  // CoList scenarios - comparing pack ON vs pack OFF
  const listScenarios = [
    { name: "10 items", count: 10 },
    { name: "25 items", count: 25 },
    { name: "50 items", count: 50 },
    { name: "100 items", count: 100 },
  ];

  const listResults = listScenarios.map(({ name, count }) => {
    const tasks = generateRealisticTasks(count);
    const taskStrings = tasks.map((t) => JSON.stringify(t));

    // Pack ON
    const listPack = group.createList();
    listPack.appendItems(taskStrings, undefined, "private", {
      disablePacking: false,
    });
    const contentPack =
      listPack.core.verified?.newContentSince(undefined) ?? [];
    const packedSize = measureContentSize(contentPack);

    // Pack OFF
    const listNoPack = group.createList();
    listNoPack.appendItems(taskStrings, undefined, "private", {
      disablePacking: true,
    });
    const contentNoPack =
      listNoPack.core.verified?.newContentSince(undefined) ?? [];
    const noPackSize = measureContentSize(contentNoPack);

    const savings = calculateSavings(noPackSize, packedSize);

    return { name, packedSize, noPackSize, savings };
  });

  // CoPlainText scenarios - pack ON vs pack OFF
  const textScenarios = [
    { name: "50 chars", count: 50 },
    { name: "100 chars", count: 100 },
    { name: "250 chars", count: 250 },
    { name: "500 chars", count: 500 },
  ];

  const textResults = textScenarios.map(({ name, count }) => {
    const article = generateRealisticArticle(5);
    const text = article.slice(0, count);

    // Pack ON
    const textPack = group.createPlainText("", null, "private");
    textPack.insertAfter(-1, text, "private", { disablePacking: false });
    const contentPack =
      textPack.core.verified?.newContentSince(undefined) ?? [];
    const packedSize = measureContentSize(contentPack);

    // Pack OFF
    const textNoPack = group.createPlainText("", null, "private");
    textNoPack.insertAfter(-1, text, "private", { disablePacking: true });
    const contentNoPack =
      textNoPack.core.verified?.newContentSince(undefined) ?? [];
    const noPackSize = measureContentSize(contentNoPack);

    const savings = calculateSavings(noPackSize, packedSize);

    return { name, packedSize, noPackSize, savings };
  });

  const summaryLines = [
    "=== Pack ON vs Pack OFF - Space Savings Summary ===",
    "",
    "CoList (task items):",
    ...listResults.map(
      ({ name, noPackSize, packedSize, savings }) =>
        `  ${name.padEnd(12)}: Pack OFF ${noPackSize.toString().padStart(6)}b → Pack ON ${packedSize.toString().padStart(6)}b (${savings}% savings)`,
    ),
    "",
    "CoPlainText (text insertion):",
    ...textResults.map(
      ({ name, noPackSize, packedSize, savings }) =>
        `  ${name.padEnd(12)}: Pack OFF ${noPackSize.toString().padStart(6)}b → Pack ON ${packedSize.toString().padStart(6)}b (${savings}% savings)`,
    ),
    "",
    "Key Findings:",
    "- Pack ON consistently reduces transaction size",
    "- Larger batches show more significant savings",
    "- Both CoList and CoPlainText benefit from packing",
  ].join("\n");

  describe(`${summaryLines}\n`, () => {
    bench("baseline - show summary", () => {});
  });
});

// ============================================================================
// SECTION 7: COPLAINTEXT DELETE OPERATIONS - Pack ON vs OFF comparison
// ============================================================================

describe("CoPlainText - Delete Operations Pack ON vs Pack OFF\n", () => {
  const account = cojson.LocalNode.internalCreateAccount({ crypto });
  const group = account.core.node.createGroup();

  describe("Delete 10 characters from text\n", () => {
    const initialText = "The quick brown fox jumps over the lazy dog";

    // Delete with pack enabled
    const textPack = group.createPlainText("", null, "private");
    textPack.insertAfter(-1, initialText, "private", {
      disablePacking: false,
    });
    textPack.deleteRange({ from: 0, to: 10 }, "private");
    const contentPack =
      textPack.core.verified?.newContentSince(undefined) ?? [];
    const packedSize = measureContentSize(contentPack);

    // Delete with pack disabled
    const textNoPack = group.createPlainText("", null, "private");
    textNoPack.insertAfter(-1, initialText, "private", {
      disablePacking: true,
    });
    textNoPack.deleteRange({ from: 0, to: 10 }, "private", {
      disablePacking: true,
    });

    const contentNoPack =
      textNoPack.core.verified?.newContentSince(undefined) ?? [];
    const noPackSize = measureContentSize(contentNoPack);

    const savings = calculateSavings(noPackSize, packedSize);

    describe(`Delete Pack ON: ${packedSize}b | Pack OFF: ${noPackSize}b | Savings: ${savings}%\n`, () => {
      bench(
        "delete 10 chars with pack=true",
        () => {
          const tempText = group.createPlainText("", null, "private");
          tempText.insertAfter(-1, initialText, "private", {
            disablePacking: false,
          });
          tempText.deleteRange({ from: 0, to: 10 }, "private");
        },
        { iterations: 1000 },
      );

      bench(
        "delete 10 chars with pack=false",
        () => {
          const tempText = group.createPlainText("", null, "private");
          tempText.insertAfter(-1, initialText, "private", {
            disablePacking: true,
          });
          tempText.deleteRange({ from: 0, to: 10 }, "private", {
            disablePacking: true,
          });
        },
        { iterations: 1000 },
      );
    });
  });

  describe("Delete 50 characters from text\n", () => {
    const article = generateRealisticArticle(5);
    const initialText = article.slice(0, 200);

    // Delete with pack enabled
    const textPack = group.createPlainText("", null, "private");
    textPack.insertAfter(-1, initialText, "private", {
      disablePacking: false,
    });
    textPack.deleteRange({ from: 20, to: 70 }, "private");
    const contentPack =
      textPack.core.verified?.newContentSince(undefined) ?? [];
    const packedSize = measureContentSize(contentPack);

    // Delete with pack disabled
    const textNoPack = group.createPlainText("", null, "private");
    textNoPack.insertAfter(-1, initialText, "private", {
      disablePacking: true,
    });
    textNoPack.deleteRange({ from: 20, to: 70 }, "private", {
      disablePacking: true,
    });

    const contentNoPack =
      textNoPack.core.verified?.newContentSince(undefined) ?? [];
    const noPackSize = measureContentSize(contentNoPack);

    const savings = calculateSavings(noPackSize, packedSize);

    describe(`Delete Pack ON: ${packedSize}b | Pack OFF: ${noPackSize}b | Savings: ${savings}%\n`, () => {
      bench(
        "delete 50 chars with pack=true",
        () => {
          const tempText = group.createPlainText("", null, "private");
          tempText.insertAfter(-1, initialText, "private", {
            disablePacking: false,
          });
          tempText.deleteRange({ from: 20, to: 70 }, "private");
        },
        { iterations: 500 },
      );

      bench(
        "delete 50 chars with pack=false",
        () => {
          const tempText = group.createPlainText("", null, "private");
          tempText.insertAfter(-1, initialText, "private", {
            disablePacking: true,
          });
          tempText.deleteRange({ from: 20, to: 70 }, "private", {
            disablePacking: true,
          });
        },
        { iterations: 500 },
      );
    });
  });

  describe("Delete 100 characters from text\n", () => {
    const article = generateRealisticArticle(10);
    const initialText = article.slice(0, 500);

    // Delete with pack enabled
    const textPack = group.createPlainText("", null, "private");
    textPack.insertAfter(-1, initialText, "private", {
      disablePacking: false,
    });
    textPack.deleteRange({ from: 50, to: 150 }, "private");
    const contentPack =
      textPack.core.verified?.newContentSince(undefined) ?? [];
    const packedSize = measureContentSize(contentPack);

    // Delete with pack disabled
    const textNoPack = group.createPlainText("", null, "private");
    textNoPack.insertAfter(-1, initialText, "private", {
      disablePacking: true,
    });
    textNoPack.deleteRange({ from: 50, to: 150 }, "private", {
      disablePacking: true,
    });

    const contentNoPack =
      textNoPack.core.verified?.newContentSince(undefined) ?? [];
    const noPackSize = measureContentSize(contentNoPack);

    const savings = calculateSavings(noPackSize, packedSize);

    describe(`Delete Pack ON: ${packedSize}b | Pack OFF: ${noPackSize}b | Savings: ${savings}%\n`, () => {
      bench(
        "delete 100 chars with pack=true",
        () => {
          const tempText = group.createPlainText("", null, "private");
          tempText.insertAfter(-1, initialText, "private", {
            disablePacking: false,
          });
          tempText.deleteRange({ from: 50, to: 150 }, "private");
        },
        { iterations: 500 },
      );

      bench(
        "delete 100 chars with pack=false",
        () => {
          const tempText = group.createPlainText("", null, "private");
          tempText.insertAfter(-1, initialText, "private", {
            disablePacking: true,
          });
          tempText.deleteRange({ from: 50, to: 150 }, "private", {
            disablePacking: true,
          });
        },
        { iterations: 500 },
      );
    });
  });

  describe("Delete 250 characters from large text\n", () => {
    const article = generateRealisticArticle(20);
    const initialText = article.slice(0, 1000);

    // Delete with pack enabled
    const textPack = group.createPlainText("", null, "private");
    textPack.insertAfter(-1, initialText, "private", {
      disablePacking: false,
    });
    textPack.deleteRange({ from: 100, to: 350 }, "private");
    const contentPack =
      textPack.core.verified?.newContentSince(undefined) ?? [];
    const packedSize = measureContentSize(contentPack);

    // Delete with pack disabled
    const textNoPack = group.createPlainText("", null, "private");
    textNoPack.insertAfter(-1, initialText, "private", {
      disablePacking: true,
    });
    textNoPack.deleteRange({ from: 100, to: 350 }, "private", {
      disablePacking: true,
    });

    const contentNoPack =
      textNoPack.core.verified?.newContentSince(undefined) ?? [];
    const noPackSize = measureContentSize(contentNoPack);

    const savings = calculateSavings(noPackSize, packedSize);

    describe(`Delete Pack ON: ${packedSize}b | Pack OFF: ${noPackSize}b | Savings: ${savings}%\n`, () => {
      bench(
        "delete 250 chars with pack=true",
        () => {
          const tempText = group.createPlainText("", null, "private");
          tempText.insertAfter(-1, initialText, "private", {
            disablePacking: false,
          });
          tempText.deleteRange({ from: 100, to: 350 }, "private");
        },
        { iterations: 200 },
      );

      bench(
        "delete 250 chars with pack=false",
        () => {
          const tempText = group.createPlainText("", null, "private");
          tempText.insertAfter(-1, initialText, "private", {
            disablePacking: true,
          });
          tempText.deleteRange({ from: 100, to: 350 }, "private", {
            disablePacking: true,
          });
        },
        { iterations: 200 },
      );
    });
  });

  describe("Multiple small deletes (5 deletes of 10 chars each)\n", () => {
    const initialText = generateRealisticArticle(5).slice(0, 300);

    // Multiple deletes with pack enabled
    const textPack = group.createPlainText("", null, "private");
    textPack.insertAfter(-1, initialText, "private", {
      disablePacking: false,
    });

    // Perform 5 delete operations
    for (let i = 0; i < 5; i++) {
      textPack.deleteRange({ from: 20, to: 30 }, "private");
    }

    const contentPack =
      textPack.core.verified?.newContentSince(undefined) ?? [];
    const packedSize = measureContentSize(contentPack);

    // Multiple deletes with pack disabled
    const textNoPack = group.createPlainText("", null, "private");
    textNoPack.insertAfter(-1, initialText, "private", {
      disablePacking: true,
    });

    // Perform 5 delete operations
    for (let i = 0; i < 5; i++) {
      textNoPack.deleteRange({ from: 20, to: 30 }, "private", {
        disablePacking: true,
      });
    }

    const contentNoPack =
      textNoPack.core.verified?.newContentSince(undefined) ?? [];
    const noPackSize = measureContentSize(contentNoPack);

    const savings = calculateSavings(noPackSize, packedSize);

    describe(`Multiple Delete Pack ON: ${packedSize}b | Pack OFF: ${noPackSize}b | Savings: ${savings}%\n`, () => {
      bench(
        "5 deletes with pack=true",
        () => {
          const tempText = group.createPlainText("", null, "private");
          tempText.insertAfter(-1, initialText, "private", {
            disablePacking: false,
          });
          for (let i = 0; i < 5; i++) {
            tempText.deleteRange({ from: 20, to: 30 }, "private");
          }
        },
        { iterations: 200 },
      );

      bench(
        "5 deletes with pack=false",
        () => {
          const tempText = group.createPlainText("", null, "private");
          tempText.insertAfter(-1, initialText, "private", {
            disablePacking: true,
          });
          for (let i = 0; i < 5; i++) {
            tempText.deleteRange({ from: 20, to: 30 }, "private", {
              disablePacking: true,
            });
          }
        },
        { iterations: 200 },
      );
    });
  });
});

// ============================================================================
// SECTION 8: COPLAINTEXT DELETE SUMMARY
// ============================================================================

describe("Summary - CoPlainText Delete Operations space savings\n", () => {
  const account = cojson.LocalNode.internalCreateAccount({ crypto });
  const group = account.core.node.createGroup();

  const deleteScenarios = [
    { name: "10 chars", from: 0, to: 10, textSize: 100 },
    { name: "50 chars", from: 20, to: 70, textSize: 200 },
    { name: "100 chars", from: 50, to: 150, textSize: 500 },
    { name: "250 chars", from: 100, to: 350, textSize: 1000 },
  ];

  const deleteResults = deleteScenarios.map(({ name, from, to, textSize }) => {
    const article = generateRealisticArticle(20);
    const initialText = article.slice(0, textSize);

    // Delete with pack ON
    const textPack = group.createPlainText("", null, "private");
    textPack.insertAfter(-1, initialText, "private", {
      disablePacking: false,
    });
    textPack.deleteRange({ from, to }, "private");
    const contentPack =
      textPack.core.verified?.newContentSince(undefined) ?? [];
    const packedSize = measureContentSize(contentPack);

    // Delete with pack OFF
    const textNoPack = group.createPlainText("", null, "private");
    textNoPack.insertAfter(-1, initialText, "private", {
      disablePacking: true,
    });
    textNoPack.deleteRange({ from, to }, "private", {
      disablePacking: true,
    });

    const contentNoPack =
      textNoPack.core.verified?.newContentSince(undefined) ?? [];
    const noPackSize = measureContentSize(contentNoPack);

    const savings = calculateSavings(noPackSize, packedSize);

    return { name, packedSize, noPackSize, savings };
  });

  const summaryLines = [
    "=== CoPlainText Delete Operations - Space Savings Summary ===",
    "",
    "Delete operations (deleteRange):",
    ...deleteResults.map(
      ({ name, noPackSize, packedSize, savings }) =>
        `  ${name.padEnd(12)}: Pack OFF ${noPackSize.toString().padStart(6)}b → Pack ON ${packedSize.toString().padStart(6)}b (${savings}% savings)`,
    ),
    "",
    "Key Findings:",
    "- Delete packing significantly reduces transaction size",
    "- Larger delete ranges show more substantial savings",
    "- Pack format: [first_delete_op, ...insertion_ids] vs individual delete ops",
    "- Critical for collaborative editing with frequent deletions",
  ].join("\n");

  describe(`${summaryLines}\n`, () => {
    bench("baseline - show delete summary", () => {});
  });
});
