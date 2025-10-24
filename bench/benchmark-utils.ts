import { faker } from "@faker-js/faker";

/**
 * Generate realistic user data using Faker
 * This is used for benchmark testing with data that resembles real-world usage
 */
export function generateRealisticData(targetSize: number): any {
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

/**
 * Generate realistic task/todo items using Faker
 */
export function generateRealisticTasks(count: number) {
  faker.seed(456);
  const tasks = [];

  for (let i = 0; i < count; i++) {
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
    tasks.push(task);
  }

  return tasks;
}

/**
 * Generate realistic article text using Faker
 */
export function generateRealisticArticle(paragraphs: number = 10): string {
  faker.seed(789);
  return faker.lorem.paragraphs(paragraphs);
}

/**
 * Measure the size of content in bytes
 */
export function measureContentSize(content: any[]): number {
  return new TextEncoder().encode(JSON.stringify(content)).length;
}

/**
 * Calculate compression savings percentage
 */
export function calculateSavings(
  originalSize: number,
  compressedSize: number,
): string {
  return ((1 - compressedSize / originalSize) * 100).toFixed(1);
}

/**
 * Import/deserialize a CoValue from content messages
 * Used for benchmarking import performance
 */
export function importCoValue(coValue: any, content: any[]) {
  coValue.core.node.getCoValue(coValue.id).unmount();
  for (const msg of content) {
    coValue.core.node.syncManager.handleNewContent(msg, "storage");
  }
  const cv = coValue.core.node.getCoValue(coValue.id);
  cv.getCurrentContent();
}

/**
 * Create a test node with account and group
 */
export function createTestNode(crypto: any) {
  const account = (crypto as any).LocalNode?.internalCreateAccount?.({
    crypto,
  });
  if (!account) {
    // Fallback for proper import
    const cojson = require("cojson");
    return cojson.LocalNode.internalCreateAccount({ crypto });
  }
  return account;
}

/**
 * Benchmark constants for consistent test configuration
 */
export const BENCHMARK_CONSTANTS = {
  // Number of updates to create transaction history
  NUM_MAP_UPDATES: 10,
  // Number of edits for text editing simulation
  NUM_TEXT_EDITS: 50,
  // Number of insertions for collaborative editing stress test
  NUM_COLLABORATIVE_EDITS: 100,
  // Number of edits for large document tests
  NUM_LARGE_DOC_EDITS: 20,
  // Deterministic seed for reproducible benchmarks
  DETERMINISTIC_SEED: 12345,
};
