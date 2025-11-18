//[!code hide]
import { Organization } from "./schema";
//[!code hide]
const organization = Organization.create({
  name: "Garden Computing",
  projects: [],
});
import { createInviteLink } from "jazz-tools/svelte";

const inviteLink = createInviteLink(organization, "writer");
