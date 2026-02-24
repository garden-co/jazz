import type { SubmitEvent } from "react";
import styles from "./SchemaHashSelect.module.css";

interface SchemaHashSelectProps {
  hashes: string[];
  onSelect: (hash: string) => void;
}

export function SchemaHashSelect({ hashes, onSelect }: SchemaHashSelectProps) {
  const handleSubmit = (event: SubmitEvent<HTMLFormElement>) => {
    event.preventDefault();
    const form = event.currentTarget;
    const select = form.elements.namedItem("schema-hash") as HTMLSelectElement;
    const hash = select?.value;
    if (hash) {
      onSelect(hash);
    }
  };

  if (hashes.length === 0) {
    return (
      <section className={styles.card}>
        <h2 className={styles.title}>No schemas available</h2>
        <p className={styles.description}>No stored schemas were found for this server.</p>
      </section>
    );
  }

  return (
    <form onSubmit={handleSubmit} className={styles.form}>
      <h2 className={styles.title}>Select schema</h2>
      <label className={styles.field}>
        Schema hash
        <select name="schema-hash" required className={styles.select}>
          <option value="">—</option>
          {hashes.map((hash) => (
            <option key={hash} value={hash}>
              {hash}
            </option>
          ))}
        </select>
      </label>
      <button type="submit" className={styles.submitButton}>
        Use schema
      </button>
    </form>
  );
}
