import { useEffect, useState } from "react";
import {
  fetchStoredMigrations,
  fetchStoredWasmSchema,
  type StoredMigrationEdge,
  type WasmSchema,
} from "jazz-tools";
import { useDevtoolsContext } from "../../contexts/devtools-context.js";
import { useStandaloneContext } from "../../contexts/standalone-context.js";

export interface SchemaCatalogState {
  currentSchema: WasmSchema;
  currentSchemaHash: string | null;
  supportsCatalogue: boolean;
  hashes: string[];
  schemas: Record<string, WasmSchema>;
  migrations: StoredMigrationEdge[];
  loading: boolean;
  error: string | null;
}

export function useSchemaCatalog(): SchemaCatalogState {
  const { wasmSchema } = useDevtoolsContext();
  const standalone = useStandaloneContext();
  const [schemas, setSchemas] = useState<Record<string, WasmSchema>>({});
  const [migrations, setMigrations] = useState<StoredMigrationEdge[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!standalone) {
      setSchemas({});
      setMigrations([]);
      setLoading(false);
      setError(null);
      return;
    }

    let active = true;
    setLoading(true);
    setError(null);

    void Promise.all([
      Promise.all(
        standalone.schemaHashes.map(async (hash) => {
          const result = await fetchStoredWasmSchema(standalone.connection.serverUrl, {
            adminSecret: standalone.connection.adminSecret,
            pathPrefix: standalone.connection.serverPathPrefix,
            schemaHash: hash,
          });

          return [hash, result.schema] as const;
        }),
      ),
      fetchStoredMigrations(standalone.connection.serverUrl, {
        adminSecret: standalone.connection.adminSecret,
        pathPrefix: standalone.connection.serverPathPrefix,
      }),
    ])
      .then(([entries, migrationResult]) => {
        if (!active) {
          return;
        }

        const nextSchemas = Object.fromEntries(entries);
        if (standalone.selectedSchemaHash) {
          nextSchemas[standalone.selectedSchemaHash] = wasmSchema;
        }

        setSchemas(nextSchemas);
        setMigrations(migrationResult.migrations);
        setLoading(false);
      })
      .catch((fetchError: unknown) => {
        if (!active) {
          return;
        }

        setError(fetchError instanceof Error ? fetchError.message : String(fetchError));
        setLoading(false);
      });

    return () => {
      active = false;
    };
  }, [
    standalone,
    standalone?.connection.adminSecret,
    standalone?.connection.serverPathPrefix,
    standalone?.connection.serverUrl,
    standalone?.schemaHashes,
    standalone?.selectedSchemaHash,
    wasmSchema,
  ]);

  if (!standalone) {
    return {
      currentSchema: wasmSchema,
      currentSchemaHash: null,
      supportsCatalogue: false,
      hashes: [],
      schemas: {},
      migrations: [],
      loading: false,
      error: null,
    };
  }

  return {
    currentSchema: wasmSchema,
    currentSchemaHash: standalone.selectedSchemaHash,
    supportsCatalogue: true,
    hashes: standalone.schemaHashes,
    schemas,
    migrations,
    loading,
    error,
  };
}
