import { co, z } from "jazz-tools";

export const Organization = co.map({
  name: z.string(),
});

// #region JoinRequest
const JoinRequest = co.map({
  account: co.account(),
  status: z.literal(["pending", "approved", "rejected"]),
});

const RequestsList = co.list(JoinRequest);
// #endregion

export { JoinRequest, RequestsList };
