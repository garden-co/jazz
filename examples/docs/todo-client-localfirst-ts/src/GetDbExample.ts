// #region writing-get-db-ts
import { createJazzClient } from "jazz-tools/client";

const session = await createJazzClient({ serverUrl: "/jazz", env: "dev", initial: "device" });
const db = session.db;
// #endregion writing-get-db-ts

void db;
