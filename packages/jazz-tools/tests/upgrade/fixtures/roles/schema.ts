import { schema as s } from "jazz-tools";
import { migrations } from "./migrations/index.js";
const schema = {
  orgs: s.table({ name: s.string() }, { members: s.reverse("members", "org"), docs: s.reverse("docs", "org") }),
  members: s.table({ orgId: s.uuid(), userId: s.string(), role: s.enum("admin", "editor", "viewer") }, { org: s.rel("orgs", "orgId") }),
  docs: s.table({ orgId: s.uuid(), title: s.string(), status: s.enum("draft", "published", "locked"), authorId: s.string() }, { org: s.rel("orgs", "orgId"), comments: s.reverse("comments", "doc"), shares: s.reverse("docShares", "doc") }),
  docShares: s.table({ docId: s.uuid(), userId: s.string() }, { doc: s.rel("docs", "docId") }),
  comments: s.table({ docId: s.uuid(), body: s.string() }, { doc: s.rel("docs", "docId") }),
};
export const app = s.defineApp(schema, { migrations });
