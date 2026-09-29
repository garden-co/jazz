"use client";

import { useState, useEffect } from "react";
import { Code } from "@/components/docs/mdx-client";
import {
  getStoredApp,
  onAppGenerated,
  restoreFromSession,
  type GeneratedApp,
} from "@/lib/generated-app-store";

export function CloudConfig() {
  const [app, setApp] = useState<GeneratedApp | null>(null);

  useEffect(() => {
    restoreFromSession();
    setApp(getStoredApp());
    return onAppGenerated(setApp);
  }, []);

  const appId = app?.appId ?? "<your-app-id>";
  const code = `{\n  appId: "${appId}",\n  serverUrl: "https://v2.sync.jazz.tools/",\n}`;

  return <Code language="ts" code={code} />;
}
