"use client";

import { useState } from "react";
import { Callout } from "fumadocs-ui/components/callout";
import { DynamicCodeBlock } from "fumadocs-ui/components/dynamic-codeblock";
import { type GeneratedApp, storeGeneratedApp } from "@/lib/generated-app-store";

function CredentialsBlock({ app }: { app: GeneratedApp }) {
  const code = [
    `JAZZ_APP_ID="${app.appId}"`,
    `JAZZ_ADMIN_SECRET="${app.adminSecret}"`,
    `JAZZ_BACKEND_SECRET="${app.backendSecret}"`,
  ].join("\n");
  return <DynamicCodeBlock lang="env" code={code} />;
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
