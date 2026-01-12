import { assert, beforeEach, describe, expect, test } from "vitest";
import { co, z } from "../../exports.js";
import { createJazzTestAccount, setupJazzTestSync } from "../../testing.js";
import { assertLoaded } from "../utils.js";

// Define the difficulty levels enum
const difficultyLevels = ["easy", "medium", "hard"] as const;

// Define the Quest schema with migration
const QuestSchema = co
  .map({
    title: z.string(),
    description: z.string(),
    imageUrl: z.string(),
    twigs: z.number(),
    difficulty: z.enum(difficultyLevels),
    category: z.string(),
    categories: z.array(z.string()),
    completed: z.boolean(),
  })
  .withMigration((quest) => {
    if (quest.categories === undefined) {
      quest.$jazz.set("categories", [quest.category]);
    }
  });

beforeEach(async () => {
  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
    creationProps: { name: "Quest Tester" },
  });
});

describe("QuestSchema", () => {
  test("should fill categories array with category when categories is undefined", async () => {
    // Create a quest without categories
    const quest = QuestSchema.create(
      // @ts-expect-error - (simulating old data)
      {
        title: "Test Quest",
        description: "A test quest description",
        imageUrl: "https://example.com/image.jpg",
        twigs: 100,
        difficulty: "medium",
        category: "adventure",
        completed: false,
      },
      {
        validation: "loose",
      },
    );

    // Initially categories should be undefined
    expect(quest.categories).toBeUndefined();

    // Load the quest to trigger the migration
    const loadedQuest = await QuestSchema.load(quest.$jazz.id);
    assertLoaded(loadedQuest);

    // After loading, the migration should have run and filled categories
    expect(loadedQuest.categories).toEqual(["adventure"]);
    expect(loadedQuest.category).toEqual("adventure");
  });

  test("should preserve existing categories when they are already defined", async () => {
    // Create a quest with categories already defined
    const quest = QuestSchema.create({
      title: "Test Quest",
      description: "A test quest description",
      imageUrl: "https://example.com/image.jpg",
      twigs: 150,
      difficulty: "hard",
      category: "combat",
      categories: ["combat", "boss-fight", "endgame"],
      completed: false,
    });

    // Categories should be defined initially
    expect(quest.categories).toEqual(["combat", "boss-fight", "endgame"]);

    // Load the quest to ensure migration doesn't change existing categories
    const loadedQuest = await QuestSchema.load(quest.$jazz.id);
    assertLoaded(loadedQuest);

    // Categories should remain unchanged after migration
    expect(loadedQuest.categories).toEqual(["combat", "boss-fight", "endgame"]);
    expect(loadedQuest.category).toEqual("combat");
  });
});
