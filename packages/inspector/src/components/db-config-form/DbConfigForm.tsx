import { fetchSchemaHashes } from "jazz-tools";
import { useEffect, useLayoutEffect, useRef, useState, type FormEvent } from "react";
import {
  normalizeSchemaHashInfos,
  type SchemaHashInfo,
} from "../../utility/schema-hash-display.js";
import styles from "./DbConfigForm.module.css";

export interface DbConfigFormValues {
  name: string;
  serverUrl: string;
  appId: string;
  adminSecret: string;
  env: string;
}

type SchemaHashesResult = Awaited<ReturnType<typeof fetchSchemaHashes>> & {
  schemas?: SchemaHashInfo[];
};

interface DbConfigFormProps {
  onSubmit: (values: DbConfigFormValues, schemas: SchemaHashInfo[]) => void;
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
  const initialName = initialValues?.name ?? "";
  const initialServerUrl = initialValues?.serverUrl ?? "";
  const initialAppId = initialValues?.appId ?? "";
  const initialAdminSecret = initialValues?.adminSecret ?? "";
  const initialEnv = initialValues?.env ?? "dev";
  const submissionGeneration = useRef(0);
  const [name, setName] = useState(initialName);
  const [serverUrl, setServerUrl] = useState(initialServerUrl);
  const [appId, setAppId] = useState(initialAppId);
  const [adminSecret, setAdminSecret] = useState(initialAdminSecret);
  const [env, setEnv] = useState(initialEnv);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  useLayoutEffect(() => {
    submissionGeneration.current += 1;
    return () => {
      submissionGeneration.current += 1;
    };
  }, [initialName, initialServerUrl, initialAppId, initialAdminSecret, initialEnv, mode]);

  useEffect(() => {
    setName(initialName);
    setServerUrl(initialServerUrl);
    setAppId(initialAppId);
    setAdminSecret(initialAdminSecret);
    setEnv(initialEnv);
    setIsSubmitting(false);
    setErrorMessage(null);
  }, [initialName, initialServerUrl, initialAppId, initialAdminSecret, initialEnv, mode]);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const submissionId = ++submissionGeneration.current;

    setErrorMessage(null);
    setIsSubmitting(true);

    const values: DbConfigFormValues = {
      name: name.trim(),
      serverUrl: serverUrl.trim(),
      appId: appId.trim(),
      adminSecret: adminSecret.trim(),
      env: env.trim() || "dev",
    };

    try {
      const { hashes, schemas } = (await fetchSchemaHashes(values.serverUrl, {
        appId: values.appId,
        adminSecret: values.adminSecret,
      })) as SchemaHashesResult;
      if (submissionGeneration.current !== submissionId) return;
      onSubmit(values, normalizeSchemaHashInfos(hashes, schemas));
    } catch (error) {
      if (submissionGeneration.current !== submissionId) return;
      const message = error instanceof Error ? error.message : String(error);
      setErrorMessage(message);
    } finally {
      if (submissionGeneration.current === submissionId) {
        setIsSubmitting(false);
      }
    }
  };

  const handleCancel = () => {
    submissionGeneration.current += 1;
    onCancel?.();
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
          <button type="button" onClick={handleCancel} className={styles.resetButton}>
            Cancel
          </button>
        ) : null}
      </div>
    </form>
  );
}
