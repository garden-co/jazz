import { describe, expect, it } from "vitest";
import { col } from "../dsl.js";
import { defineApp, defineTable } from "../typed-app.js";
import {
  analyzeRelations,
  createRelationCatalogue,
  validateRelationCatalogue,
} from "./relation-analyzer.js";

const suffixDefinition = {
  categories: defineTable({}),
  people: defineTable({}),
  analyses: defineTable({}),
  statuses: defineTable({}),
  links: defineTable({
    category_id: col.ref("categories"),
    personId: col.ref("people"),
    analysis_ids: col.array(col.ref("analyses")),
    statusIds: col.array(col.ref("statuses")),
    address: col.ref("people"),
  }),
};
const suffixSchema = defineApp(suffixDefinition).wasmSchema;
describe("relation catalogue", () => {
  it("retains runtime pluralizer names across every reference suffix branch", () => {
    const links = analyzeRelations(suffixSchema).get("links");
    expect(links?.filter((relation) => relation.type === "forward").map((r) => r.name)).toEqual([
      "category",
      "person",
      "analyses",
      "statuses",
      "addresses",
    ]);

    expect(analyzeRelations(suffixSchema).get("categories")?.[0]?.name).toBe("linksViaCategory");
    expect(
      analyzeRelations(suffixSchema)
        .get("people")
        ?.map((r) => r.name),
    ).toEqual(["linksViaPerson", "linksViaAddresses"]);
    expect(analyzeRelations(suffixSchema).get("analyses")?.[0]?.name).toBe("linksViaAnalyses");
    expect(analyzeRelations(suffixSchema).get("statuses")?.[0]?.name).toBe("linksViaStatuses");
  });

  it("fails closed when a catalogue is stale for its schema", () => {
    const catalogue = createRelationCatalogue(suffixSchema);
    const changedSchema = defineApp({
      ...suffixDefinition,
      links: defineTable({
        category_id: col.ref("categories"),
        personId: col.ref("people"),
        analysis_ids: col.array(col.ref("analyses")),
        statusIds: col.array(col.ref("statuses")),
        address: col.ref("people"),
        label: col.string(),
      }),
    }).wasmSchema;
    const tamperedCatalogue = {
      ...catalogue,
      relations: { ...catalogue.relations, links: [] },
    };
    expect(() => validateRelationCatalogue(suffixSchema, tamperedCatalogue)).toThrow(
      "Relation catalogue is stale",
    );

    expect(() => validateRelationCatalogue(changedSchema, catalogue)).toThrow(
      "Relation catalogue is stale",
    );
    expect(() => validateRelationCatalogue(suffixSchema, catalogue)).not.toThrow();
  });

  it("validates a generated catalogue while constructing a typed app", () => {
    const definition = {
      users: { name: col.string() },
      posts: { user_ids: col.array(col.ref("users")) },
    };
    const baseApp = defineApp(definition);
    const catalogue = createRelationCatalogue(baseApp.wasmSchema);

    expect(() => defineApp(definition, catalogue)).not.toThrow();
    expect(() =>
      defineApp(
        {
          ...definition,
          posts: {
            ...definition.posts,
            title: col.string(),
          },
        },
        catalogue,
      ),
    ).toThrow("Relation catalogue is stale");
  });
});
