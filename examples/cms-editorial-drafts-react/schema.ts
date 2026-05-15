import { schema as s } from "jazz-tools";

const schema = {
  branches: s.table({
    name: s.string(),
    description: s.string().default(""),
    owner_id: s.string(),
  }),
  articles: s
    .table({
      articleKey: s.string(),
      revision: s.int(),
      title: s.string(),
      slug: s.string(),
      excerpt: s.string().default(""),
      body: s.string().default(""),
      status: s.enum("draft", "review", "published").default("draft"),
      category: s.enum("news", "engineering", "design", "product", "culture").default("news"),
      tags: s.array(s.string()).default([]),
      hero_color: s.string().default("#6366f1"),
      featured: s.boolean().default(false),
      deleted: s.boolean().default(false),
      owner_id: s.string(),
    })
    .index("by_article_key", ["articleKey"]),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

export type Article = s.RowOf<typeof app.articles>;
export type Branch = s.RowOf<typeof app.branches>;

export const CATEGORIES = ["news", "engineering", "design", "product", "culture"] as const;
export const STATUSES = ["draft", "review", "published"] as const;
export const HERO_COLORS = [
  "#6366f1",
  "#ec4899",
  "#f59e0b",
  "#10b981",
  "#3b82f6",
  "#ef4444",
  "#8b5cf6",
  "#14b8a6",
] as const;
