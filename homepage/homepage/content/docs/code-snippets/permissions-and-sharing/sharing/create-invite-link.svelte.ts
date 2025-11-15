import { Organization } from "./schema";
const organization = Organization.create({
  name: "Garden Computing",
  projects: [],
});

// #region Basic
import { createInviteLink } from "jazz-tools/svelte";
const inviteLink = createInviteLink(organization, "writer");
// #endregion
