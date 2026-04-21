"use client";

import { useState, useEffect } from "react";
import { DynamicCodeBlock } from "fumadocs-ui/components/dynamic-codeblock";
import { getStoredApp, onAppGenerated, type GeneratedApp } from "@/lib/generated-app-store";

export function DeployCommand() {
  const [app, setApp] = useState<GeneratedApp | null>(null);

  useEffect(() => {
    setApp(getStoredApp());
    return onAppGenerated(setApp);
  }, []);

  const appId = app?.appId ?? "<your-app-id>";
  const adminSecret = app?.adminSecret ?? "<your-admin-secret>";

  const code = [
    "pnpm dlx jazz-tools@alpha deploy \\",
    `  ${appId} \\`,
    `  --server-url https://v2.sync.jazz.tools/ \\`,
    `  --admin-secret ${adminSecret}`,
  ].join("\n");

  return <DynamicCodeBlock lang="bash" code={code} />;
}
