import { Organization } from "./schema";
const organization = Organization.create({ name: "Garden Computing" });

// #region CreateInviteLink
import { createInviteLink } from "jazz-tools/react";

createInviteLink(organization, "writer"); // or reader, admin, writeOnly
// #endregion

// #region AcceptInvite
import { useAcceptInvite } from "jazz-tools/react";

useAcceptInvite({
  invitedObjectSchema: Organization,
  onAccept: (organizationID) => {
    console.log("Accepted invite!");
    // navigate to the organization page
  },
});
// #endregion
