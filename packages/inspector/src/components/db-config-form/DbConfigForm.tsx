import { fetchSchemaHashes } from "jazz-tools";
import { useState, type FormEvent } from "react";
import styles from "./DbConfigForm.module.css";

export interface DbConfigFormValues {
  serverUrl: string;
  appId: string;
  adminSecret: string;
  env: string;
  branch: string;
  serverPathPrefix?: string;
}

interface DbConfigFormProps {
  onSubmit: (values: DbConfigFormValues, hashes: string[]) => void;
  initialValues?: Partial<DbConfigFormValues>;
}

export function DbConfigForm({ onSubmit, initialValues }: DbConfigFormProps) {
  const [serverUrl, setServerUrl] = useState(initialValues?.serverUrl ?? "");
  const [appId, setAppId] = useState(initialValues?.appId ?? "");
  const [adminSecret, setAdminSecret] = useState(initialValues?.adminSecret ?? "");
  const [env, setEnv] = useState(initialValues?.env ?? "dev");
  const [branch, setBranch] = useState(initialValues?.branch ?? "main");
  const [serverPathPrefix, setServerPathPrefix] = useState(initialValues?.serverPathPrefix ?? "");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setErrorMessage(null);
    setIsSubmitting(true);

    const values: DbConfigFormValues = {
      serverUrl: serverUrl.trim(),
      appId: appId.trim(),
      adminSecret: adminSecret.trim(),
      env: env.trim() || "dev",
      branch: branch.trim() || "main",
      serverPathPrefix: serverPathPrefix.trim() || undefined,
    };

    try {
      const { hashes } = await fetchSchemaHashes(values.serverUrl, {
        adminSecret: values.adminSecret,
        pathPrefix: values.serverPathPrefix,
      });
      onSubmit(values, hashes);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      setErrorMessage(message);
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <form onSubmit={handleSubmit} className={styles.form}>
      <h2 className={styles.title}>Connect to Jazz server</h2>
      <label className={styles.field}>
        Server URL
        <input
          type="url"
          value={serverUrl}
          onChange={(e) => setServerUrl(e.target.value)}
          required
          placeholder="https://example.com"
          className={styles.input}
        />
      </label>
      <label className={styles.field}>
        App ID
        <input
          type="text"
          value={appId}
          onChange={(e) => setAppId(e.target.value)}
          required
          className={styles.input}
        />
      </label>
      <label className={styles.field}>
        Admin secret
        <input
          type="password"
          value={adminSecret}
          onChange={(e) => setAdminSecret(e.target.value)}
          required
          className={styles.input}
        />
      </label>
      <label className={styles.field}>
        Env
        <input
          type="text"
          value={env}
          onChange={(e) => setEnv(e.target.value)}
          placeholder="dev"
          className={styles.input}
        />
      </label>
      <label className={styles.field}>
        Branch
        <input
          type="text"
          value={branch}
          onChange={(e) => setBranch(e.target.value)}
          placeholder="main"
          className={styles.input}
        />
      </label>
      <label className={styles.field}>
        Path prefix <span className={styles.optionalText}>(optional)</span>
        <input
          type="text"
          value={serverPathPrefix}
          onChange={(e) => setServerPathPrefix(e.target.value)}
          placeholder="/apps/&lt;appId&gt;"
          className={styles.input}
        />
      </label>
      {errorMessage ? (
        <p role="alert" className={styles.errorText}>
          {errorMessage}
        </p>
      ) : null}
      <button type="submit" disabled={isSubmitting} className={styles.submitButton}>
        {isSubmitting ? "Fetching schemas…" : "Connect"}
      </button>
    </form>
  );
}
