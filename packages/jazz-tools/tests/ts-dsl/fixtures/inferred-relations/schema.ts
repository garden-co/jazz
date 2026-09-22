import { schema as s } from "jazz-tools";

export const schema = {
  categories: s.table(
    { label: s.string() },
    {
      teamsViaCategory: s.reverse("teams", "category"),
      recordsViaCategory: s.reverse("records", "category"),
      recordsViaCategories: s.reverse("records", "categories"),
    },
  ),
  people: s.table(
    { name: s.string() },
    {
      recordsViaPerson: s.reverse("records", "person"),
      recordsViaAddress: s.reverse("records", "addressRelation"),
      recordsViaPeople: s.reverse("records", "people"),
    },
  ),
  analyses: s.table(
    { summary: s.string() },
    { recordsViaAnalyses: s.reverse("records", "analyses") },
  ),
  statuses: s.table(
    { label: s.string() },
    { recordsViaStatuses: s.reverse("records", "statuses") },
  ),
  teams: s.table(
    { category_id: s.uuid() },
    {
      category: s.rel("categories", "category_id"),
      recordsViaTeam: s.reverse("records", "team"),
      recordsViaTeams: s.reverse("records", "teams"),
    },
  ),
  records: s.table(
    {
      category_id: s.uuid(),
      personId: s.uuid(),
      team_id: s.uuid(),
      teamIds: s.array(s.uuid()),
      address: s.uuid().optional(),
      category_ids: s.array(s.uuid()),
      person_ids: s.array(s.uuid()),
      analysis_ids: s.array(s.uuid()),
      status_ids: s.array(s.uuid()),
    },
    {
      category: s.rel("categories", "category_id"),
      person: s.rel("people", "personId"),
      team: s.rel("teams", "team_id"),
      teams: s.rel("teams", "teamIds"),
      addressRelation: s.rel("people", "address"),
      categories: s.rel("categories", "category_ids"),
      people: s.rel("people", "person_ids"),
      analyses: s.rel("analyses", "analysis_ids"),
      statuses: s.rel("statuses", "status_ids"),
    },
  ),
};
export type AppSchema = s.Schema<typeof schema>;
