import { useState } from "react";
import { DbConfigForm, SchemaHashSelect } from "#db-config-form/index";
import type { DbConfigFormValues } from "#db-config-form/index";
import type { StoredConnection } from "#lib/config/connections";
import { useStandaloneConnection } from "#contexts/standalone-connection-context";
import styles from "../../App.module.css";

const DEFAULT_INITIAL_VALUES = { serverUrl: "https://v2.sync.jazz.tools/" } as const;

interface ConnectionFormPageProps {
  mode: "connect" | "edit";
  connection?: StoredConnection;
}

export function ConnectionFormPage({
  mode,
  connection,
}: ConnectionFormPageProps): React.ReactElement {
  const { fragmentConfig, connections, saveConnectionAndOpen, manageConnections } =
    useStandaloneConnection();
  const [formValues, setFormValues] = useState<DbConfigFormValues | null>(null);
  const [schemaHashes, setSchemaHashes] = useState<string[]>([]);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  if (mode === "edit" && connection === undefined) {
    return (
      <main className={styles.connectionPage}>
        <section className={styles.stateCard}>
          <h2 className={styles.stateTitle}>Connection not found</h2>
          <p className={styles.loadingText}>This saved connection is no longer available.</p>
          <button type="button" onClick={manageConnections} className={styles.actionButton}>
            Manage connections
          </button>
        </section>
      </main>
    );
  }

  if (schemaHashes.length > 0 && formValues !== null) {
    return (
      <main className={styles.connectionPage}>
        <section className={styles.connectionFormCard}>
          <SchemaHashSelect
            hashes={schemaHashes}
            onSelect={(schemaHash) => saveConnectionAndOpen(formValues, schemaHash, connection?.id)}
          />
        </section>
      </main>
    );
  }

  const initialValues = formValues ?? connection ?? fragmentConfig ?? DEFAULT_INITIAL_VALUES;
  const title =
    mode === "edit"
      ? "Edit connection"
      : connections.length > 0
        ? "Add connection"
        : "Connect to Jazz server";
  const cancelHandler = mode === "edit" || connections.length > 0 ? manageConnections : undefined;

  return (
    <main className={styles.connectionPage}>
      <section className={styles.connectionFormCard}>
        <DbConfigForm
          onSubmit={(values, hashes) => {
            setErrorMessage(null);

            if (hashes.length === 0) {
              setErrorMessage("No stored schemas were found for this server.");
              return;
            }

            if (hashes.length === 1) {
              void saveConnectionAndOpen(values, hashes[0], connection?.id);
              return;
            }

            setFormValues(values);
            setSchemaHashes(hashes);
          }}
          initialValues={initialValues}
          mode={mode}
          title={title}
          onCancel={cancelHandler}
        />
        {errorMessage !== null ? (
          <p className={styles.errorText} role="alert">
            {errorMessage}
          </p>
        ) : null}
      </section>
    </main>
  );
}
