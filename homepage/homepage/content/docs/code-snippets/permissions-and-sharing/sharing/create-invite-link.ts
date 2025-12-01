//[!code hide]
import { JazzAccount, Organization } from "./schema.ts";
const me = await JazzAccount.getMe().$jazz.ensureLoaded({
  resolve: {
    root: {
      organizations: true,
    },
  },
});
const organization = Organization.create({
  name: "Garden Computing",
  projects: [],
});

// #region CreateLink
import { createInviteLink } from "jazz-tools";

const inviteLink = createInviteLink(
  organization,
  "writer",
  "https://example.com/", // Base URL for the invite link
);
// #endregion

// #region ConsumeLink
import { consumeInviteLink } from "jazz-tools";

consumeInviteLink({
  inviteURL: inviteLink,
  invitedObjectSchema: Organization, // Pass the schema for the invited object
}).then(async (invitedObject) => {
  if (!invitedObject) throw new Error("Failed to consume invite link");
  const organization = await Organization.load(invitedObject?.valueID);
  me.root.organizations.$jazz.push(organization);
});
// #endregion
