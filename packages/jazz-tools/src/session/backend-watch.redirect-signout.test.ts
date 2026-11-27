import { afterEach, expect, it, vi } from "vitest";
import { AccountManager, type AccountHandle, type AccountIdentity } from "../accounts/state.js";
import {
  browserBackendWatchEvents,
  NAVIGATION_CHECK_MS,
  watchBackendUser,
  type BackendWatchEvents,
} from "./backend-watch.js";
import { createJazzSessionOwner } from "./state.js";

// Sign-outs the app makes by redirecting (AuthKit's signOut(), a server
// action, Auth.js) never call Jazz: the other tabs, and the tab itself after
// a client-side redirect, follow without the app's help.

const user = (subject: string) =>
  Object.freeze({
    id: `jazz:backend:${subject}`,
    identity: { issuer: "jazz:backend", subject },
  }) as AccountHandle;
const settle = async (ms = 0) => {
  await new Promise((resolve) => setTimeout(resolve, ms));
  for (let i = 0; i < 20; i++) await Promise.resolve();
};
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

/** One browser: tabs share its cookie (the backend's answer), a channel and the last-answer entry. */
function browser(options: { noteAnswers?: boolean } = {}) {
  let backendUser: AccountHandle | null = null;
  let lastAnswer: string | undefined;
  const listeners = new Set<() => void>();
  const tabs: { close(): Promise<void> }[] = [];
  async function tab(initial: AccountHandle | undefined) {
    const accounts = new AccountManager(
      {
        createLocalFirst: () => {
          throw new Error("no guests here");
        },
        restoreLocalFirst: () => {
          throw new Error("no guests here");
        },
        loginWithBackend: async () => {
          if (!backendUser) throw new Error("signed_out");
          return backendUser;
        },
        logout() {},
      },
      initial,
    );
    const session = await createJazzSessionOwner({
      accounts,
      clearOnSignOut: true,
      openClient: async () => ({ db: {}, shutdown: async () => {} }),
    });
    let navigate: (() => void) | undefined;
    let mine: (() => void) | undefined;
    const probe = vi.fn(async () => backendUser?.identity ?? null);
    const posts = vi.fn(() => {
      for (const listener of listeners) if (listener !== mine) listener();
    });
    const events: BackendWatchEvents = {
      onFocus: () => () => {},
      onNavigate(check) {
        navigate = check;
        return () => (navigate = undefined);
      },
      crossTab: {
        post: posts,
        listen(check) {
          mine = check;
          listeners.add(check);
          return () => listeners.delete(check);
        },
      },
      isVisible: () => true,
      ...(options.noteAnswers === false
        ? {}
        : {
            noteAnswer(answer: AccountIdentity | null) {
              const value = answer ? `${answer.issuer}:${answer.subject}` : "none";
              const changed = lastAnswer !== undefined && lastAnswer !== value;
              lastAnswer = value;
              return changed;
            },
          }),
    };
    const watch = watchBackendUser(session, probe, events, { confirmMs: 10 });
    const t = {
      session,
      probe,
      posts,
      navigate: () => navigate?.(),
      subject: () => session.getSnapshot().account?.identity.subject ?? "signed-out",
      async close() {
        watch.stop();
        await session.close();
      },
    };
    tabs.push(t);
    return t;
  }
  return {
    tab,
    signIn: (next: AccountHandle | null) => (backendUser = next),
    close: () => Promise.all(tabs.map((t) => t.close())),
  };
}

it("a signed-out page after a redirect signs out the other tabs, without focus", async () => {
  const b = browser();
  b.signIn(user("alice"));
  const first = await b.tab(user("alice"));
  const second = await b.tab(user("alice"));
  await settle(30);
  expect([first.subject(), second.subject()]).toEqual(["alice", "alice"]);
  // Same answer as before: no tab is told anything.
  expect(first.posts).not.toHaveBeenCalled();
  expect(second.posts).not.toHaveBeenCalled();

  // The app's sign-out redirects; with clearOnSignOut the page it lands on
  // opens signed out, so its own account never changes.
  b.signIn(null);
  const landing = await b.tab(undefined);
  await settle(60);
  expect(landing.subject()).toBe("signed-out");
  expect(landing.posts).toHaveBeenCalled();
  expect([first.subject(), second.subject()]).toEqual(["signed-out", "signed-out"]);
  await b.close();
});

it("without noted answers, the other tabs stay signed in (the gap this closes)", async () => {
  const b = browser({ noteAnswers: false });
  b.signIn(user("alice"));
  const other = await b.tab(user("alice"));
  await settle(30);
  b.signIn(null);
  await b.tab(undefined);
  await settle(60);
  expect(other.subject()).toBe("alice");
  await b.close();
});

it("a client-side redirect after sign-out signs the tab itself out, and then the others", async () => {
  const b = browser();
  b.signIn(user("alice"));
  const here = await b.tab(user("alice"));
  const other = await b.tab(user("alice"));
  await settle(30);

  // A server action cleared the cookie and redirected within the app: no
  // page load, no focus.
  b.signIn(null);
  here.navigate();
  await settle(60);
  expect(here.subject()).toBe("signed-out");
  expect(other.subject()).toBe("signed-out");
  await b.close();
});

it("a navigation that finds the same user signed in changes nothing", async () => {
  const b = browser();
  b.signIn(user("alice"));
  const here = await b.tab(user("alice"));
  await settle(30);
  here.navigate();
  await settle(30);
  expect(here.probe).toHaveBeenCalledTimes(2);
  expect(here.subject()).toBe("alice");
  expect(here.posts).not.toHaveBeenCalled();
  await b.close();
});

/** Globals a browser page gives `browserBackendWatchEvents`. */
function stubPage() {
  const store = new Map<string, string>();
  const page = { visible: true, href: "https://app.test/" };
  const navigation = new EventTarget();
  vi.stubGlobal("localStorage", {
    getItem: (key: string) => store.get(key) ?? null,
    setItem: (key: string, value: string) => void store.set(key, value),
  });
  vi.stubGlobal("document", {
    get visibilityState() {
      return page.visible ? "visible" : "hidden";
    },
  });
  vi.stubGlobal("location", {
    get href() {
      return page.href;
    },
  });
  vi.stubGlobal("navigation", navigation);
  return {
    store,
    page,
    go(href: string) {
      page.href = href;
      navigation.dispatchEvent(new Event("currententrychange"));
    },
  };
}

it("browser: notes a hash of who signed in, tells others only on a change, and never from a hidden page", async () => {
  const page = stubPage();
  const events = browserBackendWatchEvents("jazz-account:test");
  const alice = { issuer: "https://auth.test", subject: "alice@example.com" };
  // Nothing noted before: nothing to differ from.
  expect(await events.noteAnswer!(alice)).toBe(false);
  expect(await events.noteAnswer!(alice)).toBe(false);
  const noted = page.store.get("jazz-account:test:answer")!;
  expect(noted).toMatch(/^user:[0-9a-f]{64}$/);
  expect(noted).not.toContain("alice");

  expect(await events.noteAnswer!(null)).toBe(true);
  expect(page.store.get("jazz-account:test:answer")).toBe("none");
  // A background tab's cookie may be stale: its answer is not noted.
  page.page.visible = false;
  expect(await events.noteAnswer!(alice)).toBe(false);
  expect(page.store.get("jazz-account:test:answer")).toBe("none");
  page.page.visible = true;
  expect(await events.noteAnswer!(alice)).toBe(true);
});

it("browser: checks on navigation to another URL, at most once per interval plus a trailing check", () => {
  vi.useFakeTimers();
  const page = stubPage();
  const events = browserBackendWatchEvents("jazz-account:test");
  const check = vi.fn();
  const stop = events.onNavigate!(check);

  page.go("https://app.test/a");
  expect(check).toHaveBeenCalledTimes(1);
  // The same URL (a router saving its scroll position) or another hash: not a move.
  page.go("https://app.test/a");
  page.go("https://app.test/a#section");
  // Moves within the interval wait for one trailing check.
  page.go("https://app.test/b");
  page.go("https://app.test/signed-out");
  expect(check).toHaveBeenCalledTimes(1);
  vi.advanceTimersByTime(NAVIGATION_CHECK_MS);
  expect(check).toHaveBeenCalledTimes(2);
  vi.advanceTimersByTime(NAVIGATION_CHECK_MS);
  expect(check).toHaveBeenCalledTimes(2);
  page.go("https://app.test/c");
  expect(check).toHaveBeenCalledTimes(3);

  page.go("https://app.test/d");
  stop();
  vi.advanceTimersByTime(NAVIGATION_CHECK_MS);
  page.go("https://app.test/e");
  expect(check).toHaveBeenCalledTimes(3);
});
