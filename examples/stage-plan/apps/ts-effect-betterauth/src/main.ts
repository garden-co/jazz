import "@garden-co/design/jazz/tokens.css";
import "@garden-co/design/jazz/fonts.css";
import "./styles.css";
import { betterAuth, createJazzApp } from "jazz-tools/client";
import { Jazz } from "jazz-tools/effect";
import { authClient } from "./auth-client";
import { h } from "./dom";
import { plannerScreen } from "./planner";
import { signInScreen } from "./sign-in";

const root = document.getElementById("app")!;
const status = (message: string, error = false) =>
  h(
    "main",
    { class: "shell" },
    h("p", { class: error ? "status error" : "status", role: error ? "alert" : "status" }, message),
  );

// Better Auth's session drives sign-in; the server's Jazz (`createJazz` in
// server/jazz.ts) reads it in `authenticate`.
const jazz = createJazzApp({
  appId: import.meta.env.VITE_JAZZ_APP_ID,
  serverUrl: "/jazz",
  auth: betterAuth(authClient),
});

let shown: { key: string; dispose?(): void } | undefined;
function show(key: string, render: () => HTMLElement | { element: HTMLElement; dispose(): void }) {
  if (shown?.key === key) return;
  shown?.dispose?.();
  const view = render();
  shown = view instanceof HTMLElement ? { key } : { key, dispose: view.dispose };
  root.replaceChildren(view instanceof HTMLElement ? view : view.element);
}

async function signOut() {
  // Signs out of Jazz first (flushing local writes), then of Better Auth.
  await jazz.logout();
  window.location.assign("/");
}

function update() {
  const session = authClient.$store.atoms.session!.get();
  if (session.isPending) return show("loading", () => status("Opening StagePlan…"));
  if (!session.data) return show("signed-out", signInScreen);
  const snapshot = jazz.getSnapshot();
  if (snapshot.status === "error")
    return show(`error:${snapshot.error?.message}`, () =>
      status(snapshot.error?.message ?? "Could not open StagePlan.", true),
    );
  const client = snapshot.status === "ready" ? snapshot.client : undefined;
  const account = client?.session?.user.account;
  if (!client || !account) return show("loading", () => status("Opening StagePlan…"));
  show(`planner:${account}`, () =>
    plannerScreen(Jazz.fromDb(client.db), account, () => void signOut()),
  );
}

authClient.$store.atoms.session!.subscribe(update);
jazz.subscribe(update);
update();
