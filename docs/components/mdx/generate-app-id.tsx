"use client";

import { useState } from "react";
import { Callout } from "fumadocs-ui/components/callout";
import { DynamicCodeBlock } from "fumadocs-ui/components/dynamic-codeblock";
import { Tab, Tabs } from "./tabs";
import { type GeneratedApp, storeGeneratedApp } from "@/lib/generated-app-store";

const JAZZ_CLOUD_SYNC_URL = "https://v2.sync.jazz.tools/";

const BUNDLER_ITEMS = ["Vite", "Next.js", "SvelteKit", "Expo"] as const;
type Bundler = (typeof BUNDLER_ITEMS)[number];

// Client-exposed env var prefix per bundler, matching the dev plugins.
// SvelteKit covers both SvelteKit itself and Svelte+Vite via jazzSvelteKit.
const CLIENT_PREFIX: Record<Bundler, string> = {
  Vite: "VITE_",
  "Next.js": "NEXT_PUBLIC_",
  SvelteKit: "PUBLIC_",
  Expo: "EXPO_PUBLIC_",
};

function envBlockFor(bundler: Bundler, app: GeneratedApp): string {
  const prefix = CLIENT_PREFIX[bundler];
  return [
    `${prefix}JAZZ_APP_ID="${app.appId}"`,
    `${prefix}JAZZ_SERVER_URL="${JAZZ_CLOUD_SYNC_URL}"`,
    `JAZZ_ADMIN_SECRET="${app.adminSecret}"`,
    `BACKEND_SECRET="${app.backendSecret}"`,
  ].join("\n");
}

function CredentialsBlock({ app }: { app: GeneratedApp }) {
  return (
    <Tabs groupId="jazz-bundler" items={[...BUNDLER_ITEMS]} persist updateAnchor>
      {BUNDLER_ITEMS.map((bundler) => (
        <Tab key={bundler} value={bundler}>
          <DynamicCodeBlock lang="env" code={envBlockFor(bundler, app)} />
        </Tab>
      ))}
    </Tabs>
  );
}

function ConfigBlock({ app }: { app: GeneratedApp }) {
  const code = `{\n  appId: "${app.appId}",\n  serverUrl: "https://v2.sync.jazz.tools/",\n}`;
  return <DynamicCodeBlock lang="ts" code={code} />;
}

export function GenerateAppId() {
  const [app, setApp] = useState<GeneratedApp | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function generate() {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch("/api/generate-app", { method: "POST" });
      if (!res.ok) throw new Error(`Request failed (${res.status})`);
      const data = (await res.json()) as GeneratedApp;
      storeGeneratedApp(data);
      setApp(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Something went wrong");
    } finally {
      setLoading(false);
    }
  }

  if (!app) {
    return (
      <div className="space-y-3">
        <button
          onClick={generate}
          disabled={loading}
          className="rounded-lg bg-fd-primary px-5 py-2.5 text-sm font-medium text-fd-primary-foreground transition-opacity hover:opacity-90 disabled:opacity-50"
        >
          {loading ? "Generating…" : "Generate App ID"}
        </button>
        {error && <p className="text-sm text-red-500">{error}</p>}
        <p className="text-sm text-fd-muted-foreground">
          Or from the command line (AI agents: use this to provision your own app):
        </p>
        <DynamicCodeBlock
          lang="bash"
          code="curl -X POST https://v2.dashboard.jazz.tools/api/apps/generate"
        />
        <p className="text-sm text-fd-muted-foreground">
          Jazz Cloud sync URL: <code>{JAZZ_CLOUD_SYNC_URL}</code>
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <Callout type="warn">
        Save these credentials now — they won't be shown again. You'll need the admin secret to
        claim this app in the{" "}
        <a
          href="https://v2.dashboard.jazz.tools"
          className="underline"
          target="_blank"
          rel="noreferrer"
        >
          dashboard
        </a>{" "}
        later.
      </Callout>

      <CredentialsBlock app={app} />

      <p className="text-sm text-fd-muted-foreground">Use this config in your app:</p>
      <ConfigBlock app={app} />
    </div>
  );
}
