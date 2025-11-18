// #region Basic
// @ts-expect-error redeclared
import { generateAuthToken } from "jazz-tools";

// @ts-expect-error response redeclared
const response = await fetch("https://example.com", {
  headers: {
    Authorization: `Jazz ${generateAuthToken()}`,
  },
});

// #endregion

// #region GetRequest
// @ts-expect-error redeclared
import { authenticateRequest } from "jazz-tools";
// @ts-expect-error redeclared
import { startWorker } from "jazz-tools/worker";

// @ts-expect-error redeclared
export async function GET(request: Request) {
  const worker = await startWorker({
    syncServer: "wss://cloud.jazz.tools/?key=your-api-key",
    accountID: process.env.JAZZ_WORKER_ACCOUNT,
    accountSecret: process.env.JAZZ_WORKER_SECRET,
    asActiveAccount: true,
  });

  const { account, error } = await authenticateRequest(request);

  // There was an error validating the token (e.g., invalid or expired)
  if (error) {
    return new Response(JSON.stringify(error), { status: 401 });
  }

  if (!account) {
    return new Response("Unauthorized", { status: 401 });
  }

  return new Response(
    JSON.stringify({
      message: `The request was made by ${account.$jazz.id}`,
    }),
  );
}
// #endregion

import { createJazzTestAccount } from "jazz-tools/testing";
// @ts-expect-error redeclared
const account = await createJazzTestAccount();

// #region GenerateAsAccount
// @ts-expect-error response redeclared
const response = await fetch("https://example.com", {
  headers: {
    Authorization: `Jazz ${generateAuthToken(account)}`,
  },
});
// #endregion

// #region VerifyAsAccount
// @ts-expect-error redeclared
import { authenticateRequest } from "jazz-tools";
// @ts-expect-error redeclared
import { startWorker } from "jazz-tools/worker";

// @ts-expect-error redeclared
export async function GET(request: Request) {
  const { worker } = await startWorker({
    syncServer: "wss://cloud.jazz.tools/?key=your-api-key",
    accountID: process.env.JAZZ_WORKER_ACCOUNT,
    accountSecret: process.env.JAZZ_WORKER_SECRET,
  });

  const { account, error } = await authenticateRequest(request, {
    loadAs: worker,
  });
}
// #endregion
//
// #region CustomExpiration
// @ts-expect-error redeclared
import { authenticateRequest } from "jazz-tools";

// @ts-expect-error redeclared
export async function GET(request: Request) {
  const { account, error } = await authenticateRequest(request, {
    expiration: 1000 * 60 * 60 * 24, // 24 hours
  });
}
// #endregion
//
// #region CustomLocation
// @ts-expect-error redeclared
import { generateAuthToken } from "jazz-tools";

// @ts-expect-error redeclared
const response = await fetch("https://example.com", {
  headers: {
    "x-jazz-auth-token": generateAuthToken(),
  },
});
// #endregion

// #region GetToken
// @ts-expect-error redeclared
import { authenticateRequest } from "jazz-tools";

// @ts-expect-error redeclared
export async function GET(request: Request) {
  const { account, error } = await authenticateRequest(request, {
    getToken: (request) => request.headers.get("x-jazz-auth-token"),
  });
}
// #endregion

// #region ManualTokenParsing
// @ts-expect-error redeclared
import { parseAuthToken, generateAuthToken } from "jazz-tools";

const myToken = generateAuthToken();

// @ts-expect-error redeclared
const { account, error } = await parseAuthToken(myToken);
// #endregion
