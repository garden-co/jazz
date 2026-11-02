"use client";

import { useState } from "react";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Code as InlineCode } from "@astryxdesign/core/Code";
import { Link } from "@astryxdesign/core/Link";
import { Text } from "@astryxdesign/core/Text";
import { Callout, Code } from "@/components/docs/mdx-client";
import { type GeneratedApp, storeGeneratedApp } from "@/lib/generated-app-store";

const JAZZ_CLOUD_SYNC_URL = "https://v2.sync.jazz.tools/";

// Everything stays in the backend's environment: browsers only talk to /jazz.
function envBlockFor(app: GeneratedApp): string {
  return [
    `JAZZ_APP_ID="${app.appId}"`,
    `JAZZ_SERVER_URL="${JAZZ_CLOUD_SYNC_URL}"`,
    `JAZZ_ADMIN_SECRET="${app.adminSecret}"`,
    `BACKEND_SECRET="${app.backendSecret}"`,
  ].join("\n");
}

function CredentialsBlock({ app }: { app: GeneratedApp }) {
  return <Code language="env" code={envBlockFor(app)} />;
}

function ConfigBlock({ app }: { app: GeneratedApp }) {
  const code = [
    `createJazz({`,
    `  // app, permissions, authenticate ...`,
    `  store: remote({`,
    `    appId: process.env.JAZZ_APP_ID!,`,
    `    url: process.env.JAZZ_SERVER_URL!,`,
    `    adminSecret: process.env.JAZZ_ADMIN_SECRET!,`,
    `    backendSecret: process.env.BACKEND_SECRET!,`,
    `  }),`,
    `});`,
  ].join("\n");
  return <Code language="ts" code={code} />;
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
        <Callout type="warn">
          Apps generated here are unclaimed. Claim the app in the dashboard within 14 days.
          Unclaimed apps are automatically deleted after 14 days.
        </Callout>
        <Button label="Generate App ID" variant="primary" isLoading={loading} onClick={generate} />
        {error && <Banner status="error" title="Could not generate an app" description={error} />}
        <Text as="p" display="block" color="secondary">
          Or from the command line (AI agents: use this to provision your own app):
        </Text>
        <Code
          language="bash"
          code="curl -X POST https://v2.dashboard.jazz.tools/api/apps/generate"
        />
        <Text as="p" display="block" color="secondary">
          Jazz Cloud sync URL: <InlineCode>{JAZZ_CLOUD_SYNC_URL}</InlineCode>
        </Text>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <Callout type="warn">
        Save these credentials now — they won't be shown again. You'll need the admin secret to
        claim this app in the{" "}
        <Link href="https://v2.dashboard.jazz.tools" target="_blank" rel="noreferrer">
          dashboard
        </Link>{" "}
        within 14 days. Unclaimed apps are automatically deleted after 14 days.
      </Callout>

      <CredentialsBlock app={app} />

      <Text as="p" display="block" color="secondary">
        Use them in your backend:
      </Text>
      <ConfigBlock app={app} />
    </div>
  );
}
