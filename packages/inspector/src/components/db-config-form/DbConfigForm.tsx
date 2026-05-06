import { fetchSchemaHashes } from "jazz-tools";
import { useEffect, useState, type FormEvent } from "react";
import styles from "./DbConfigForm.module.css";

export interface DbConfigFormValues {
  name: string;
  serverUrl: string;
  appId: string;
  adminSecret: string;
  env: string;
  branch: string;
}

interface DbConfigFormProps {
  onSubmit: (values: DbConfigFormValues, hashes: string[]) => void;
  initialValues?: Partial<DbConfigFormValues>;
  mode?: "connect" | "edit";
  title?: string;
  onCancel?: () => void;
}

export function DbConfigForm({
  onSubmit,
  initialValues,
  mode = "connect",
  title,
  onCancel,
}: DbConfigFormProps) {
  const [name, setName] = useState(initialValues?.name ?? "");
  const [serverUrl, setServerUrl] = useState(initialValues?.serverUrl ?? "");
  const [appId, setAppId] = useState(initialValues?.appId ?? "");
  const [adminSecret, setAdminSecret] = useState(initialValues?.adminSecret ?? "");
  const [env, setEnv] = useState(initialValues?.env ?? "dev");
  const [branch, setBranch] = useState(initialValues?.branch ?? "main");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  useEffect(() => {
    setName(initialValues?.name ?? "");
    setServerUrl(initialValues?.serverUrl ?? "");
    setAppId(initialValues?.appId ?? "");
    setAdminSecret(initialValues?.adminSecret ?? "");
    setEnv(initialValues?.env ?? "dev");
    setBranch(initialValues?.branch ?? "main");
    setIsSubmitting(false);
    setErrorMessage(null);
  }, [initialValues]);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setErrorMessage(null);
    setIsSubmitting(true);

    const values: DbConfigFormValues = {
      name: name.trim(),
      serverUrl: serverUrl.trim(),
      appId: appId.trim(),
      adminSecret: adminSecret.trim(),
      env: env.trim() || "dev",
      branch: branch.trim() || "main",
    };

    try {
      const { hashes } = await fetchSchemaHashes(values.serverUrl, {
        appId: values.appId,
        adminSecret: values.adminSecret,
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
      <h2 className={styles.title}>
        {title ?? (mode === "edit" ? "Edit connection" : "Connect to Jazz server")}
      </h2>
      <label className={styles.field}>
        Name
        <input
          type="text"
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="Local dev"
          className={styles.input}
        />
      </label>
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
      {errorMessage ? (
        <p role="alert" className={styles.errorText}>
          {errorMessage}
        </p>
      ) : null}
      <div className={styles.buttonRow}>
        <button type="submit" disabled={isSubmitting} className={styles.submitButton}>
          {isSubmitting ? "Fetching schemas…" : mode === "edit" ? "Save changes" : "Connect"}
        </button>
        {onCancel ? (
          <button type="button" onClick={onCancel} className={styles.resetButton}>
            Cancel
          </button>
        ) : null}
      </div>
    </form>
  );
}
