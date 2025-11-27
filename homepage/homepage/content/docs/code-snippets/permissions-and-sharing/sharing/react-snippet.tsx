import { JazzAccount, Organization } from "./schema";
const organization = Organization.create({
  name: "Garden Computing",
  projects: [],
});

const me = await JazzAccount.getMe().$jazz.ensureLoaded({
  resolve: {
    root: {
      organizations: true,
    },
  },
});

// #region CreateInviteLink
import { createInviteLink } from "jazz-tools/react";

const inviteLink = createInviteLink(organization, "writer"); // or reader, admin, writeOnly
// #endregion

// #region AcceptInvite
import { useAcceptInvite } from "jazz-tools/react";

useAcceptInvite({
  invitedObjectSchema: Organization,
  onAccept: async (organizationID) => {
    const organization = await Organization.load(organizationID);
    if (!organization.$isLoaded)
      throw new Error("Organization could not be loaded");
    me.root.organizations.$jazz.push(organization);
    // navigate to the organization page
  },
});
// #endregion
