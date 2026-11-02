import "@garden-co/design/jazz/tokens.css";
import "@garden-co/design/jazz/fonts.css";
import "./styles.css";
import { betterAuth, createJazzApp } from "jazz-tools/client";
import { bootstrapWorkspace } from "@/src/lib/account-enrollment";
import { authClient } from "./auth-client";
import { chatScreen } from "./chat";
import { h } from "./dom";
import { signInScreen } from "./sign-in";

const root = document.getElementById("app")!;
const appId = import.meta.env.VITE_JAZZ_APP_ID;

const status = (message: string, error = false) =>
  h(
    "main",
    { class: "status-screen" },
    h("p", { role: error ? "alert" : "status", class: error ? "error" : "" }, message),
  );

// Better Auth's session drives sign-in; the server's Jazz (`createJazz` in
// src/lib/backend-jazz-client.ts) reads it in `authenticate`.
const jazz = createJazzApp({ appId, serverUrl: "/jazz", auth: betterAuth(authClient) });

let shown: { key: string; dispose?(): void } | undefined;
function show(key: string, render: () => HTMLElement | { element: HTMLElement; dispose(): void }) {
  if (shown?.key === key) return;
  shown?.dispose?.();
  const view = render();
  if (view instanceof HTMLElement) {
    shown = { key };
    root.replaceChildren(view);
  } else {
    shown = { key, dispose: view.dispose };
    root.replaceChildren(view.element);
  }
}

let prepared: { userId: string; result: Promise<{ ok: boolean; agentLabel: string }> } | undefined;
/** First open: the server seeds this user's workspace (idempotent). */
function prepare(userId: string) {
  if (prepared?.userId !== userId)
    prepared = {
      userId,
      result: Promise.all([
        bootstrapWorkspace().then(
          (response) => response.ok,
          () => false,
        ),
        fetch("/api/config")
          .then((response) => response.json() as Promise<{ agentLabel: string }>)
          .then(
            (config) => config.agentLabel,
            () => "Agent",
          ),
      ]).then(([ok, agentLabel]) => ({ ok, agentLabel })),
    };
  return prepared.result;
}

async function signOut() {
  // Signs out of Jazz first (flushing local writes), then of Better Auth.
  await jazz.logout();
  window.location.assign("/");
}

function update() {
  const session = authClient.$store.atoms.session!.get();
  if (session.isPending) return show("loading", () => status("Opening MusicAgent…"));
  if (!session.data) return show("signed-out", signInScreen);
  const user = session.data.user as { id: string; name: string };
  const snapshot = jazz.getSnapshot();
  if (snapshot.status === "error")
    return show(`error:${snapshot.error?.message}`, () =>
      status(snapshot.error?.message ?? "Could not open MusicAgent.", true),
    );
  const client = snapshot.status === "ready" ? snapshot.client : undefined;
  const account = client?.session?.user.account;
  if (!client || !account) return show("loading", () => status("Opening MusicAgent…"));
  const key = `chat:${user.id}:${account}`;
  if (shown?.key === key) return;
  show(`preparing:${key}`, () => status("Preparing your booking desk…"));
  void prepare(user.id).then(({ ok, agentLabel }) => {
    if (jazz.getSnapshot().client !== client) return;
    if (!ok)
      return show(`failed:${key}`, () => status("The workspace could not be prepared.", true));
    show(key, () =>
      chatScreen({
        db: client.db,
        account,
        userName: user.name,
        agentLabel,
        onSignOut: () => void signOut(),
      }),
    );
  });
}

authClient.$store.atoms.session!.subscribe(update);
jazz.subscribe(update);
update();
