import { co, z } from "jazz-tools";

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

// #region AcceptInvite
import { useAcceptInviteNative } from "jazz-tools/expo";

useAcceptInviteNative({
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

// #region CreateInviteLink
import { createInviteLink } from "jazz-tools/expo";

const inviteLink = createInviteLink(organization, "writer");
// #endregion
