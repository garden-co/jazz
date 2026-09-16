import { schema as s } from "jazz-tools";

export const schema = {
  categories: s.table({ label: s.string() }),
  people: s.table({ name: s.string() }),
  analyses: s.table({ summary: s.string() }),
  statuses: s.table({ label: s.string() }),
  teams: s.table({ category_id: s.ref("categories") }),
  records: s.table({
    category_id: s.ref("categories"),
    personId: s.ref("people"),
    team_id: s.ref("teams"),
    teamIds: s.array(s.ref("teams")),
    address: s.ref("people").optional(),
    category_ids: s.array(s.ref("categories")),
    person_ids: s.array(s.ref("people")),
    analysis_ids: s.array(s.ref("analyses")),
    status_ids: s.array(s.ref("statuses")),
  }),
};
export type AppSchema = s.Schema<typeof schema>;
