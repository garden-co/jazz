import styles from "./index.module.css";

interface LiveQueryFiltersProps {
  availableTables: string[];
  selectedTable: string;
  selectedTier: string;
  onTableChange: (value: string) => void;
  onTierChange: (value: string) => void;
  showTierFilter?: boolean;
}

const TIER_OPTIONS = [
  { value: "", label: "All tiers" },
  { value: "worker", label: "worker" },
  { value: "edge", label: "edge" },
  { value: "global", label: "global" },
] as const;

export function LiveQueryFilters({
  availableTables,
  selectedTable,
  selectedTier,
  onTableChange,
  onTierChange,
  showTierFilter = true,
}: LiveQueryFiltersProps) {
  return (
    <form className={styles.filters} onSubmit={(event) => event.preventDefault()}>
      <label className={styles.filterField}>
        Table
        <select
          aria-label="Filter by table"
          className={styles.filterSelect}
          value={selectedTable}
          onChange={(event) => onTableChange(event.target.value)}
        >
          <option value="">All tables</option>
          {availableTables.map((table) => (
            <option key={table} value={table}>
              {table}
            </option>
          ))}
        </select>
      </label>
      {showTierFilter ? (
        <label className={styles.filterField}>
          Tier
          <select
            aria-label="Filter by tier"
            className={styles.filterSelect}
            value={selectedTier}
            onChange={(event) => onTierChange(event.target.value)}
          >
            {TIER_OPTIONS.map((tier) => (
              <option key={tier.value || "all"} value={tier.value}>
                {tier.label}
              </option>
            ))}
          </select>
        </label>
      ) : null}
    </form>
  );
}
