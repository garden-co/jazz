import { describe, bench } from "vitest";
import { faker } from "@faker-js/faker";

import * as cojson from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { NapiCrypto } from "cojson/crypto/NapiCrypto";

const crypto = await WasmCrypto.create();
const napiCrypto = await NapiCrypto.create();

// Different payload sizes to test compression effectiveness
// Note: Compression is now explicit with compress: true, no automatic threshold
const PAYLOAD_SIZES = {
  SMALL: 512, // Small payload
  THRESHOLD: 1024, // 1KB payload
  MEDIUM: 4096, // 4KB - good compression ratio
  LARGE: 100 * 1024, // 100KB - maximum recommended tx size
};

// Generate realistic user data using Faker
function generateRealisticData(targetSize: number): any {
  faker.seed(123); // Fixed seed for consistent benchmarks

  const users = [];
  const comments = [];
  const posts = [];

  let currentSize = 0;
  let id = 1;

  while (currentSize < targetSize) {
    // Generate realistic user
    const user = {
      id: faker.string.uuid(),
      firstName: faker.person.firstName(),
      lastName: faker.person.lastName(),
      email: faker.internet.email(),
      age: faker.number.int({ min: 18, max: 80 }),
      phone: faker.phone.number(),
      city: faker.location.city(),
      country: faker.location.country(),
      address: {
        street: faker.location.streetAddress(),
        zipCode: faker.location.zipCode(),
        state: faker.location.state(),
      },
      company: faker.company.name(),
      jobTitle: faker.person.jobTitle(),
      registeredAt: faker.date.past({ years: 2 }).toISOString(),
      active: faker.datatype.boolean(),
      avatar: faker.image.avatar(),
      bio: faker.lorem.paragraph(),
      preferences: {
        notifications: faker.datatype.boolean(),
        newsletter: faker.datatype.boolean(),
        theme: faker.helpers.arrayElement(["dark", "light", "auto"]),
        language: faker.helpers.arrayElement(["en", "es", "fr", "de", "it"]),
      },
    };
    users.push(user);

    // Generate realistic post
    const post = {
      id: faker.string.uuid(),
      authorId: users[faker.number.int({ min: 0, max: users.length - 1 })]?.id,
      title: faker.lorem.sentence(),
      content: faker.lorem.paragraphs({ min: 2, max: 5 }),
      excerpt: faker.lorem.paragraph(),
      createdAt: faker.date.past({ years: 1 }).toISOString(),
      updatedAt: faker.date.recent({ days: 30 }).toISOString(),
      publishedAt: faker.date.recent({ days: 60 }).toISOString(),
      likes: faker.number.int({ min: 0, max: 1000 }),
      views: faker.number.int({ min: 0, max: 10000 }),
      shares: faker.number.int({ min: 0, max: 500 }),
      tags: faker.helpers.arrayElements(
        [
          "technology",
          "science",
          "business",
          "health",
          "travel",
          "food",
          "sports",
          "entertainment",
        ],
        { min: 1, max: 4 },
      ),
      category: faker.helpers.arrayElement([
        "blog",
        "news",
        "tutorial",
        "review",
      ]),
      featured: faker.datatype.boolean(),
      metadata: {
        readTime: faker.number.int({ min: 1, max: 20 }),
        difficulty: faker.helpers.arrayElement([
          "beginner",
          "intermediate",
          "advanced",
        ]),
      },
    };
    posts.push(post);

    // Generate realistic comment
    const comment = {
      id: faker.string.uuid(),
      postId: posts[faker.number.int({ min: 0, max: posts.length - 1 })]?.id,
      userId: users[faker.number.int({ min: 0, max: users.length - 1 })]?.id,
      text: faker.lorem.paragraph({ min: 1, max: 3 }),
      createdAt: faker.date.recent({ days: 7 }).toISOString(),
      upvotes: faker.number.int({ min: 0, max: 100 }),
      downvotes: faker.number.int({ min: 0, max: 50 }),
      edited: faker.datatype.boolean(),
      editedAt: faker.datatype.boolean()
        ? faker.date.recent({ days: 3 }).toISOString()
        : null,
      replies: faker.number.int({ min: 0, max: 10 }),
    };
    comments.push(comment);

    currentSize = JSON.stringify({ users, posts, comments }).length;
    id++;
  }

  return {
    users,
    posts,
    comments,
    metadata: {
      totalRecords: id - 1,
      generatedAt: new Date().toISOString(),
      version: "1.0.0",
    },
  };
}

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

  // Make several updates to create transaction history
  for (let i = 0; i < 10; i++) {
    map.set(
      `key_${i}`,
      JSON.stringify(testData),
      usePrivate ? "private" : "trusting",
      { compress },
    );
  }

  return { map, group };
}

function measureContentSize(content: any[]): number {
  return new TextEncoder().encode(JSON.stringify(content)).length;
}

describe("Compression overhead - Small payloads (512B)", () => {
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

  console.log(
    `[Small 512B] Private: ${sizePrivate} bytes, Trusting: ${sizeTrusting} bytes, Private+Compressed: ${sizeCompressed} bytes`,
  );

  function importMap(map: any, content: any) {
    map.core.node.getCoValue(map.id).unmount();
    for (const msg of content) {
      map.core.node.syncManager.handleNewContent(msg, "storage");
    }
    const coValue = map.core.node.getCoValue(map.id);
    coValue.getCurrentContent();
  }

  bench(
    "private (encrypted, no compression)",
    () => {
      importMap(mapPrivate, contentPrivate);
    },
    { iterations: 200 },
  );

  bench(
    "trusting (no encryption, no compression)",
    () => {
      importMap(mapTrusting, contentTrusting);
    },
    { iterations: 200 },
  );

  bench(
    "private with compress:true",
    () => {
      importMap(mapCompressed, contentCompressed);
    },
    { iterations: 200 },
  );
});

describe("Compression overhead - 1KB payloads", () => {
  const { map: mapPrivate } = generateFixtures(
    crypto,
    PAYLOAD_SIZES.THRESHOLD,
    true,
    false,
  );
  const { map: mapTrusting } = generateFixtures(
    crypto,
    PAYLOAD_SIZES.THRESHOLD,
    false,
    false,
  );
  const { map: mapCompressed } = generateFixtures(
    crypto,
    PAYLOAD_SIZES.THRESHOLD,
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
  const savings = ((1 - sizeCompressed / sizePrivate) * 100).toFixed(1);

  console.log(
    `[1KB] Private: ${sizePrivate} bytes, Trusting: ${sizeTrusting} bytes, Private+Compressed: ${sizeCompressed} bytes (${savings}% savings)`,
  );

  function importMap(map: any, content: any) {
    map.core.node.getCoValue(map.id).unmount();
    for (const msg of content) {
      map.core.node.syncManager.handleNewContent(msg, "storage");
    }
    const coValue = map.core.node.getCoValue(map.id);
    coValue.getCurrentContent();
  }

  bench(
    "private (encrypted, no compression)",
    () => {
      importMap(mapPrivate, contentPrivate);
    },
    { iterations: 200 },
  );

  bench(
    "trusting (no encryption, no compression)",
    () => {
      importMap(mapTrusting, contentTrusting);
    },
    { iterations: 200 },
  );

  bench(
    "private with compress:true",
    () => {
      importMap(mapCompressed, contentCompressed);
    },
    { iterations: 200 },
  );
});

describe("Compression overhead - Medium payloads (4KB)", () => {
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
  const savings = ((1 - sizeCompressed / sizePrivate) * 100).toFixed(1);

  console.log(
    `[4KB] Private: ${sizePrivate} bytes, Trusting: ${sizeTrusting} bytes, Private+Compressed: ${sizeCompressed} bytes (${savings}% savings)`,
  );

  function importMap(map: any, content: any) {
    map.core.node.getCoValue(map.id).unmount();
    for (const msg of content) {
      map.core.node.syncManager.handleNewContent(msg, "storage");
    }
    const coValue = map.core.node.getCoValue(map.id);
    coValue.getCurrentContent();
  }

  bench(
    "private (encrypted, no compression)",
    () => {
      importMap(mapPrivate, contentPrivate);
    },
    { iterations: 200 },
  );

  bench(
    "trusting (no encryption, no compression)",
    () => {
      importMap(mapTrusting, contentTrusting);
    },
    { iterations: 200 },
  );

  bench(
    "private with compress:true",
    () => {
      importMap(mapCompressed, contentCompressed);
    },
    { iterations: 200 },
  );
});

describe("Compression overhead - Large payloads (100KB)", () => {
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
  const savings = ((1 - sizeCompressed / sizePrivate) * 100).toFixed(1);

  console.log(
    `[100KB] Private: ${sizePrivate} bytes, Trusting: ${sizeTrusting} bytes, Private+Compressed: ${sizeCompressed} bytes (${savings}% savings)`,
  );

  function importMap(map: any, content: any) {
    map.core.node.getCoValue(map.id).unmount();
    for (const msg of content) {
      map.core.node.syncManager.handleNewContent(msg, "storage");
    }
    const coValue = map.core.node.getCoValue(map.id);
    coValue.getCurrentContent();
  }

  bench(
    "private (encrypted, no compression)",
    () => {
      importMap(mapPrivate, contentPrivate);
    },
    { iterations: 100 },
  );

  bench(
    "trusting (no encryption, no compression)",
    () => {
      importMap(mapTrusting, contentTrusting);
    },
    { iterations: 100 },
  );

  bench(
    "private with compress:true",
    () => {
      importMap(mapCompressed, contentCompressed);
    },
    { iterations: 100 },
  );
});

describe("Compression with NAPI crypto - Medium payloads (4KB)", () => {
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

  function importMap(map: any, content: any) {
    map.core.node.getCoValue(map.id).unmount();
    for (const msg of content) {
      map.core.node.syncManager.handleNewContent(msg, "storage");
    }
    const coValue = map.core.node.getCoValue(map.id);
    coValue.getCurrentContent();
  }

  bench(
    "NAPI - private (encrypted, no compression)",
    () => {
      importMap(mapPrivateNapi, contentPrivate);
    },
    { iterations: 200 },
  );

  bench(
    "NAPI - trusting (no encryption, no compression)",
    () => {
      importMap(mapTrustingNapi, contentTrusting);
    },
    { iterations: 200 },
  );

  bench(
    "NAPI - private with compress:true",
    () => {
      importMap(mapCompressedNapi, contentCompressed);
    },
    { iterations: 200 },
  );
});

describe("Transaction creation overhead with compression", () => {
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

describe("List operations with compression - comparing data types", () => {
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
  const savings = ((1 - sizeCompressed / sizePrivate) * 100).toFixed(1);

  console.log(
    `[List] Private: ${sizePrivate} bytes, Trusting: ${sizeTrusting} bytes, Private+Compressed: ${sizeCompressed} bytes (${savings}% savings)`,
  );

  function importList(list: any, content: any) {
    list.core.node.getCoValue(list.id).unmount();
    for (const msg of content) {
      list.core.node.syncManager.handleNewContent(msg, "storage");
    }
    const coValue = list.core.node.getCoValue(list.id);
    coValue.getCurrentContent();
  }

  bench(
    "list import - private (encrypted, no compression)",
    () => {
      importList(listPrivate, contentPrivate);
    },
    { iterations: 200 },
  );

  bench(
    "list import - trusting (no encryption, no compression)",
    () => {
      importList(listTrusting, contentTrusting);
    },
    { iterations: 200 },
  );

  bench(
    "list import - private with compress:true",
    () => {
      importList(listCompressed, contentCompressed);
    },
    { iterations: 200 },
  );
});

describe("Text operations with compression", () => {
  faker.seed(789);
  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  // Use realistic text content from Faker
  const initialText = faker.lorem.paragraphs(10);
  const textPrivate = groupPrivate.createPlainText(initialText);

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const textTrusting = groupTrusting.createPlainText(initialText);

  // Make realistic edits (simulating actual text editing)
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
    { min: 50, max: 50 },
  );
  for (let i = 0; i < 50; i++) {
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

  console.log(
    `[Text] Private: ${sizePrivate} bytes, Trusting: ${sizeTrusting} bytes`,
  );

  function importText(text: any, content: any) {
    text.core.node.getCoValue(text.id).unmount();
    for (const msg of content) {
      text.core.node.syncManager.handleNewContent(msg, "storage");
    }
    const coValue = text.core.node.getCoValue(text.id);
    coValue.getCurrentContent();
  }

  bench(
    "text import - private (compressed)",
    () => {
      importText(textPrivate, contentPrivate);
    },
    { iterations: 200 },
  );

  bench(
    "text import - trusting (uncompressed)",
    () => {
      importText(textTrusting, contentTrusting);
    },
    { iterations: 200 },
  );
});

describe("CoPlainText - Long document (10KB)", () => {
  // Simulate a long document like an article or documentation
  const longText = `
    Lorem ipsum dolor sit amet, consectetur adipiscing elit. 
    Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
    `.repeat(200); // ~10KB of text

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
  const ratio = ((1 - sizePrivate / sizeTrusting) * 100).toFixed(1);

  console.log(
    `[CoPlainText 10KB] Private: ${sizePrivate} bytes, Trusting: ${sizeTrusting} bytes, Savings: ${ratio}%`,
  );

  function importText(text: any, content: any) {
    text.core.node.getCoValue(text.id).unmount();
    for (const msg of content) {
      text.core.node.syncManager.handleNewContent(msg, "storage");
    }
    const coValue = text.core.node.getCoValue(text.id);
    coValue.getCurrentContent();
  }

  bench(
    "import long document - private (compressed)",
    () => {
      importText(textPrivate, contentPrivate);
    },
    { iterations: 200 },
  );

  bench(
    "import long document - trusting (uncompressed)",
    () => {
      importText(textTrusting, contentTrusting);
    },
    { iterations: 200 },
  );
});

describe("CoPlainText - Collaborative editing (many small edits)", () => {
  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const textPrivate = groupPrivate.createPlainText(
    "Initial text",
    null,
    "private",
  );

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const textTrusting = groupTrusting.createPlainText(
    "Initial text",
    null,
    "trusting",
  );

  // Simulate collaborative editing: many small insertions and deletions
  for (let i = 0; i < 100; i++) {
    const pos = Math.min(i, textPrivate.entries().length);
    textPrivate.insertAfter(pos, "x", "private");
    textTrusting.insertAfter(pos, "x", "trusting");
  }

  const contentPrivate =
    textPrivate.core.verified?.newContentSince(undefined) ?? [];
  const contentTrusting =
    textTrusting.core.verified?.newContentSince(undefined) ?? [];

  const sizePrivate = measureContentSize(contentPrivate);
  const sizeTrusting = measureContentSize(contentTrusting);

  console.log(
    `[CoPlainText many edits] Private: ${sizePrivate} bytes, Trusting: ${sizeTrusting} bytes`,
  );

  function importText(text: any, content: any) {
    text.core.node.getCoValue(text.id).unmount();
    for (const msg of content) {
      text.core.node.syncManager.handleNewContent(msg, "storage");
    }
    const coValue = text.core.node.getCoValue(text.id);
    coValue.getCurrentContent();
  }

  bench(
    "import with many edits - private",
    () => {
      importText(textPrivate, contentPrivate);
    },
    { iterations: 200 },
  );

  bench(
    "import with many edits - trusting",
    () => {
      importText(textTrusting, contentTrusting);
    },
    { iterations: 200 },
  );
});

describe("CoPlainText - Real-time editing performance", () => {
  const accountPrivate = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const textPrivate = groupPrivate.createPlainText(
    "The quick brown fox jumps over the lazy dog. ".repeat(20),
  );

  const accountTrusting = cojson.LocalNode.internalCreateAccount({ crypto });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const textTrusting = groupTrusting.createPlainText(
    "The quick brown fox jumps over the lazy dog. ".repeat(20),
  );

  bench(
    "insert single char - private (encrypted + potentially compressed)",
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
    "insert word - private (encrypted + potentially compressed)",
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

describe("CoPlainText - Large document with NAPI", () => {
  // Test with NAPI crypto for better performance
  const largeParagraph = `
    In a world where collaboration is key, real-time text editing has become 
    essential for teams working together. The ability to see changes as they 
    happen, without conflicts or overwrites, is what makes modern collaborative 
    tools so powerful. This is especially important for documentation, code 
    reviews, and creative writing where multiple people need to contribute 
    simultaneously.
  `.repeat(50); // ~15KB

  const accountPrivate = cojson.LocalNode.internalCreateAccount({
    crypto: napiCrypto,
  });
  const groupPrivate = accountPrivate.core.node.createGroup();
  const textPrivate = groupPrivate.createPlainText(
    largeParagraph,
    null,
    "private",
  );

  const accountTrusting = cojson.LocalNode.internalCreateAccount({
    crypto: napiCrypto,
  });
  const groupTrusting = accountTrusting.core.node.createGroup();
  const textTrusting = groupTrusting.createPlainText(
    largeParagraph,
    null,
    "trusting",
  );

  // Add some edits
  for (let i = 0; i < 20; i++) {
    const pos = Math.floor((textPrivate.entries().length * i) / 20);
    textPrivate.insertAfter(pos, " [edited]", "private");
    textTrusting.insertAfter(pos, " [edited]", "trusting");
  }

  const contentPrivate =
    textPrivate.core.verified?.newContentSince(undefined) ?? [];
  const contentTrusting =
    textTrusting.core.verified?.newContentSince(undefined) ?? [];

  const sizePrivate = measureContentSize(contentPrivate);
  const sizeTrusting = measureContentSize(contentTrusting);
  const ratio = ((1 - sizePrivate / sizeTrusting) * 100).toFixed(1);

  console.log(
    `[CoPlainText NAPI 15KB] Private: ${sizePrivate} bytes, Trusting: ${sizeTrusting} bytes, Savings: ${ratio}%`,
  );

  function importText(text: any, content: any) {
    text.core.node.getCoValue(text.id).unmount();
    for (const msg of content) {
      text.core.node.syncManager.handleNewContent(msg, "storage");
    }
    const coValue = text.core.node.getCoValue(text.id);
    coValue.getCurrentContent();
  }

  bench(
    "NAPI: import large document - private (compressed)",
    () => {
      importText(textPrivate, contentPrivate);
    },
    { iterations: 200 },
  );

  bench(
    "NAPI: import large document - trusting (uncompressed)",
    () => {
      importText(textTrusting, contentTrusting);
    },
    { iterations: 200 },
  );
});

describe("CoPlainText - Delete operations with compression", () => {
  function setupTextWithContent(crypto: any, usePrivate: boolean) {
    const account = cojson.LocalNode.internalCreateAccount({ crypto });
    const group = account.core.node.createGroup();
    const text = group.createPlainText(
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ".repeat(40), // ~1KB
      null,
      usePrivate ? "private" : "trusting",
    );
    return text;
  }

  const textPrivate = setupTextWithContent(crypto, true);
  const textTrusting = setupTextWithContent(crypto, false);

  bench(
    "delete range - private (encrypted + potentially compressed)",
    () => {
      const len = textPrivate.entries().length;
      if (len > 10) {
        textPrivate.deleteRange({ from: len - 10, to: len - 5 }, "private");
      }
    },
    { iterations: 500 },
  );

  bench(
    "delete range - trusting (no overhead)",
    () => {
      const len = textTrusting.entries().length;
      if (len > 10) {
        textTrusting.deleteRange({ from: len - 10, to: len - 5 }, "trusting");
      }
    },
    { iterations: 500 },
  );
});
