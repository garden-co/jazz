import { BrowserRouter } from "react-router";
import { createJazzClient, JazzProvider } from "jazz-tools/react";
import { fetchStoredWasmSchema } from "jazz-tools";
import { useEffect, useState } from "react";
import { ConfigResetProvider } from "./contexts/config-reset-context.js";
import { DevtoolsProvider } from "./contexts/devtools-context.js";
import { InspectorRoutes } from "./routes.js";
import { DbConfigForm, SchemaHashSelect } from "./components/db-config-form/index.js";
import type { DbConfigFormValues } from "./components/db-config-form/index.js";
import styles from "./App.module.css";

interface StoredConfig {
  serverUrl: string;
  appId: string;
  adminSecret: string;
  env: string;
  branch: string;
  schemaHash: string;
  serverPathPrefix?: string;
}

const STORAGE_KEY = "jazz-inspector-standalone-config";

type OnboardingStep = "form" | "schema" | null;

export default function App() {
  const [storedConfig, setStoredConfig] = useState<StoredConfig | null>(() => readStoredConfig());
  const [onboardingStep, setOnboardingStep] = useState<OnboardingStep>(
    storedConfig ? null : "form",
  );
  const [formValues, setFormValues] = useState<DbConfigFormValues | null>(null);
  const [schemaHashes, setSchemaHashes] = useState<string[]>([]);
  const [client, setClient] = useState<Awaited<ReturnType<typeof createJazzClient>> | null>(null);
  const [wasmSchema, setWasmSchema] = useState<import("jazz-tools").WasmSchema | null>(null);
  const [error, setError] = useState<string | null>(null);

  const handleFormSubmit = (values: DbConfigFormValues, hashes: string[]) => {
    setFormValues(values);
    setSchemaHashes(hashes);
    setOnboardingStep("schema");
  };

  const handleSchemaSelect = (schemaHash: string) => {
    if (!formValues) return;
    const config: StoredConfig = {
      serverUrl: formValues.serverUrl,
      appId: formValues.appId,
      adminSecret: formValues.adminSecret,
      env: formValues.env || "dev",
      branch: formValues.branch || "main",
      schemaHash,
      serverPathPrefix: formValues.serverPathPrefix,
    };
    writeStoredConfig(config);
    setStoredConfig(config);
    setFormValues(null);
    setSchemaHashes([]);
    setOnboardingStep(null);
    window.location.reload();
  };

  const handleReset = () => {
    clearStoredConfig();
    setStoredConfig(null);
    setClient(null);
    setWasmSchema(null);
    setOnboardingStep("form");
    setFormValues(null);
    setSchemaHashes([]);
  };

  useEffect(() => {
    if (!storedConfig) return;

    let active = true;

    const run = async () => {
      try {
        const [resolvedClient, { schema }] = await Promise.all([
          createJazzClient({
            appId: storedConfig.appId,
            serverUrl: storedConfig.serverUrl,
            serverPathPrefix: storedConfig.serverPathPrefix,
            env: storedConfig.env,
            userBranch: storedConfig.branch,
            adminSecret: storedConfig.adminSecret,
            driver: { type: "memory" },
          }),
          fetchStoredWasmSchema(storedConfig.serverUrl, {
            adminSecret: storedConfig.adminSecret,
            schemaHash: storedConfig.schemaHash,
            pathPrefix: storedConfig.serverPathPrefix,
          }),
        ]);

        if (!active) {
          void resolvedClient.shutdown();
          return;
        }

        setClient(resolvedClient);
        setWasmSchema(schema);
        setError(null);
      } catch (err) {
        if (!active) return;
        const message = err instanceof Error ? err.message : String(err);
        setError(message);
      }
    };

    run();

    return () => {
      active = false;
    };
  }, [storedConfig]);

  if (onboardingStep === "form") {
    return (
      <main className={styles.statePage}>
        <DbConfigForm onSubmit={handleFormSubmit} />
      </main>
    );
  }

  if (onboardingStep === "schema" && formValues) {
    return (
      <main className={styles.statePage}>
        <SchemaHashSelect hashes={schemaHashes} onSelect={handleSchemaSelect} />
      </main>
    );
  }

  if (error) {
    return (
      <main className={styles.statePage}>
        <section className={styles.stateCard}>
          <h2 className={styles.stateTitle}>Connection error</h2>
          <p role="alert" className={styles.errorText}>
            {error}
          </p>
          <button type="button" onClick={handleReset} className={styles.actionButton}>
            Reset connection
          </button>
        </section>
      </main>
    );
  }

  if (!client || !wasmSchema) {
    return (
      <main className={styles.statePage}>
        <section className={styles.stateCard}>
          <p className={styles.loadingText}>Loading...</p>
        </section>
      </main>
    );
  }

  return (
    <JazzProvider client={client}>
      <DevtoolsProvider wasmSchema={wasmSchema} runtime="standalone">
        <ConfigResetProvider onReset={handleReset}>
          <BrowserRouter>
            <InspectorRoutes />
          </BrowserRouter>
        </ConfigResetProvider>
      </DevtoolsProvider>
    </JazzProvider>
  );
}

function readStoredConfig(): StoredConfig | null {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as unknown;
    if (
      typeof parsed === "object" &&
      parsed !== null &&
      typeof (parsed as StoredConfig).serverUrl === "string" &&
      typeof (parsed as StoredConfig).appId === "string" &&
      typeof (parsed as StoredConfig).adminSecret === "string" &&
      typeof (parsed as StoredConfig).schemaHash === "string"
    ) {
      return parsed as StoredConfig;
    }
    return null;
  } catch {
    return null;
  }
}

function writeStoredConfig(config: StoredConfig): void {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(config));
}

function clearStoredConfig(): void {
  localStorage.removeItem(STORAGE_KEY);
}
