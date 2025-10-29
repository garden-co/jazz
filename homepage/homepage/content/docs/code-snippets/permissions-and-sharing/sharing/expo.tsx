import { co, z } from "jazz-tools";

import { Organization } from "./schema";
const organization = Organization.create({ name: "Garden Computing" });

// #region AcceptInvite
import { useAcceptInviteNative } from "jazz-tools/expo";

useAcceptInviteNative({
  invitedObjectSchema: Organization,
  onAccept: (organizationID) => {
    console.log("Accepted invite!");
    // navigate to the organization page
  },
});
// #endregion
