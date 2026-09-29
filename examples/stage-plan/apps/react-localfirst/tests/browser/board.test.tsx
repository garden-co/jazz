/**
 * Browser test of the board flow. Mounts the real <App /> in Chromium against
 * a local Jazz server, drives it through the DOM, and checks that a second
 * client (a crew member who joined with the invite link) sees the changes.
 */
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, it } from "vitest";
import { createAccountManager, createDb, type Db } from "jazz-tools";
import { App } from "../../src/App.js";
import { app } from "../../schema.js";
import { APP_ID, serverUrl } from "./test-constants.js";

async function waitFor<T>(
  check: () => T | null | undefined | false,
  message: string,
  timeoutMs = 15000,
) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = check();
    if (value) return value;
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 50));
    });
  }
  throw new Error(`Timed out: ${message}`);
}

function typeInto(input: HTMLInputElement | HTMLTextAreaElement, value: string) {
  const proto = input instanceof HTMLTextAreaElement ? HTMLTextAreaElement : HTMLInputElement;
  Object.getOwnPropertyDescriptor(proto.prototype, "value")!.set!.call(input, value);
  input.dispatchEvent(new Event("input", { bubbles: true }));
}

function column(root: HTMLElement, status: string) {
  return root.querySelector<HTMLElement>(`[data-status="${status}"]`);
}

function cardTitles(root: HTMLElement, status: string) {
  return [...(column(root, status)?.querySelectorAll("[data-task-id]") ?? [])].map(
    (card) => card.textContent ?? "",
  );
}

function buttonByText(root: ParentNode, text: string) {
  return [...root.querySelectorAll<HTMLElement>("button, a")].find(
    (el) => el.textContent?.trim() === text || el.getAttribute("aria-label") === text,
  );
}

describe("StagePlan board", () => {
  let root: Root | undefined;
  let container: HTMLDivElement | undefined;
  let crewDb: Db | undefined;

  afterEach(async () => {
    await act(async () => root?.unmount());
    container?.remove();
    await crewDb?.shutdown();
    window.location.hash = "";
  });

  it("runs the stage-prep flow and syncs it to a crew member", async () => {
    const accounts = await createAccountManager({ appId: APP_ID, serverUrl: serverUrl() });
    const chiefAccount = accounts.createLocalFirst();

    window.location.hash = "#/";
    container = document.createElement("div");
    document.body.appendChild(container);
    root = createRoot(container);
    await act(async () => {
      root!.render(
        <App
          config={{
            appId: APP_ID,
            serverUrl: serverUrl(),
            account: chiefAccount,
            driver: { type: "persistent", dbName: crypto.randomUUID() },
          }}
        />,
      );
    });
    const el = container;

    // First run seeds the demo show.
    const showCard = await waitFor(
      () => buttonByText(el, "The Late Lanterns: album launch"),
      "demo show card",
    );
    await act(async () => showCard.click());
    await waitFor(() => cardTitles(el, "todo").some((t) => t.includes("Soundcheck")), "demo board");
    expect(cardTitles(el, "done").join()).toContain("Load-in");

    // Add a task: it lands at the end of To do.
    const newTask = el.querySelector<HTMLInputElement>('input[placeholder^="Add a task"]')!;
    await act(async () => typeInto(newTask, "Tape the set list down"));
    await act(async () => buttonByText(el, "Add task")!.click());
    await waitFor(
      () => cardTitles(el, "todo").at(-1)?.includes("Tape the set list down"),
      "new task in To do",
    );

    // Keyboard move: focus a card and press the right arrow.
    const soundcheck = [
      ...column(el, "todo")!.querySelectorAll<HTMLElement>("[data-task-id]"),
    ].find((card) => card.textContent?.includes("Soundcheck"))!;
    const link = soundcheck.querySelector<HTMLElement>("a")!;
    link.focus();
    await act(async () => {
      link.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowRight", bubbles: true }));
    });
    await waitFor(
      () => cardTitles(el, "doing").some((t) => t.includes("Soundcheck")),
      "Soundcheck moved to In progress",
    );

    // Task detail: comment, then see it in the activity.
    await act(async () => link.click());
    const commentBox = await waitFor(
      () =>
        document.querySelector<HTMLTextAreaElement>(
          'textarea[placeholder="Add a comment for the crew"]',
        ),
      "task dialog",
    );
    await act(async () => typeInto(commentBox, "Band arrives at five"));
    await act(async () => buttonByText(document, "Comment")!.click());
    await waitFor(
      () => document.body.textContent?.includes("Band arrives at five"),
      "comment shown",
    );
    await waitFor(
      () => document.body.textContent?.includes("commented on “Soundcheck with the band”"),
      "activity entry",
    );

    // A crew member joins with the invite link and sees the board live.
    window.location.hash = window.location.hash.replace(/\/tasks\/.*$/, "/crew");
    const inviteInput = await waitFor(
      () =>
        [...el.querySelectorAll<HTMLInputElement>("input")].find((input) =>
          input.value.includes("#/join/"),
        ),
      "invite link",
    );
    const [, showId, code] = inviteInput.value.split("#/join/")[1].match(/^([^/]+)\/(.+)$/)!;

    const crewAccount = accounts.createLocalFirst();
    crewDb = await createDb({
      appId: APP_ID,
      serverUrl: serverUrl(),
      account: crewAccount,
      driver: { type: "memory" },
    });
    const crewId = crewDb.getAuthState().session?.user.account;
    if (!crewId) throw new Error("crew client has no session");
    const profile = await crewDb
      .insert(app.crew, { account: crewId, name: "Cole" })
      .wait({ tier: "global" });
    await crewDb
      .insert(app.showCrew, {
        showId,
        crewId: profile.id,
        account: crewId,
        role: "crew",
        inviteCode: code,
      })
      .wait({ tier: "global" });

    const crewTasks = async () => crewDb!.all(app.tasks.where({ showId }), { tier: "remote" });
    await expect.poll(async () => (await crewTasks()).length, { timeout: 15000 }).toBe(9);

    // The chief's crew list picks up the new member live.
    await waitFor(() => el.textContent?.includes("Cole"), "new crew member listed");

    // The crew member moves a task; the chief's board follows.
    const lineCheck = (await crewTasks()).find((task) => task.title === "Line check")!;
    await crewDb.update(app.tasks, lineCheck.id, { status: "done" }).wait({ tier: "global" });
    window.location.hash = `#/shows/${showId}`;
    await waitFor(
      () => cardTitles(el, "done").some((t) => t.includes("Line check")),
      "crew move synced",
    );
  });
});
