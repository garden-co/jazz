import { expect, test } from "@playwright/test";
import { LocalNode } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { createWorkerAccount } from "jazz-run/createWorkerAccount";
import { createJazzTestAccount } from "jazz-tools/testing";

import { co, z } from "jazz-tools";

// const account = await createWorkerAccount({
//   name: "Inspector test account",
//   peer: "wss://cloud.jazz.tools/?key=inspector-test@jazz.tools",
// });
const account = {
  accountID: "co_zevdCJMtJtXzEGHU9aiyzDfBXdb",
  agentSecret:
    "sealerSecret_z8eSD4wL3o6Uf8BnbTjX6NViKj43Q3SmEx4Yp2gpegrUR/signerSecret_zHybFXaWd8AHeqoxW91ZzTnhQFeAa9vh7mWnGHtwgSdAi",
};

// const Issue = co.map({
//   title: z.string(),
//   status: z.enum(["open", "closed"]),
//   labels: co.list(z.string()),
// })
//
// const Project = co.map({
//   name: z.string(),
//   description: z.string(),
//   issues: co.list(Issue),
// })
//
// const Organization = co.map({
//   name: z.string(),
//   projects: co.list(Project),
// })
//
// const organization = Organization.create({
//   name: "Garden Computing",
//   projects: co.list(Issue).create([
//
//   ]),
//   issues: [
//
//   ]
// })
//
// const projectsData = [
//   {
//     name: "Jazz",
//     description: "Jazz is a framework for building collaborative apps.",
//     issues: [
//       { title: "Issue 1", status: "open", labels: ["bug", "feature"] },
//       { title: "Issue 2", status: "closed", labels: ["bug"] },
//       { title: "Issue 3", status: "open", labels: ["feature", "enhancement"] },
//     ]
//   }
// ]

test("should add and delete account in dropdown", async ({ page }) => {
  const { accountID, agentSecret } = account;

  await page.goto("/");
  await page.getByLabel("Account ID").fill(accountID);
  await page.getByLabel("Account secret").fill(agentSecret);
  await page.getByRole("button", { name: "Add account" }).click();

  await expect(page.getByText("Jazz CoValue Inspector")).toBeVisible();
  await page
    .getByLabel("Account to inspect")
    .selectOption(`Inspector test account <${accountID}>`);

  await page.getByRole("button", { name: "Remove account" }).click();
  await expect(page.getByText("Jazz CoValue Inspector")).not.toBeVisible();
  await expect(page.getByText("Add an account to inspect")).toBeVisible();
  await expect(
    page.getByText(`Inspector test account <${accountID}>`),
  ).not.toBeVisible();
});

test("should inspect account", async ({ page }) => {
  const { accountID, agentSecret } = account;

  await page.goto("/");
  await page.getByLabel("Account ID").fill(accountID);
  await page.getByLabel("Account secret").fill(agentSecret);
  await page.getByRole("button", { name: "Add account" }).click();
  await page.getByRole("button", { name: "Inspect my account" }).click();

  await expect(page.getByRole("heading", { name: accountID })).toBeVisible();
  await expect(page.getByText("👤 Account")).toBeVisible();

  await page.getByRole("button", { name: "profile {} CoMap name:" }).click();
  await expect(page.getByText("Role: admin")).toBeVisible();
});
