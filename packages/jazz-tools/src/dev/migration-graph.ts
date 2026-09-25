import { appScopedUrl } from "../runtime/url.js";

export type GraphPresence = "local" | "server" | "both";

export interface MigrationGraph {
  activeSchemaHash: string | null;
  schemas: string[];
  migrations: Array<{
    fromHash: string;
    toHash: string;
    presence?: GraphPresence;
    automatic?: boolean;
  }>;
  schemaPresence?: Map<string, GraphPresence>;
  localSchemaHash?: string | null;
}

export interface LocalMigrationGraph {
  schemas: string[];
  migrations: Array<{ fromHash: string; toHash: string }>;
  currentSchemaHash: string | null;
}

export function combineMigrationGraphs(
  server: MigrationGraph,
  local: LocalMigrationGraph,
): MigrationGraph {
  const localSchemas = new Set(local.schemas);
  const serverSchemas = new Set(server.schemas);
  const schemas = [...new Set([...server.schemas, ...local.schemas])].sort();
  const presence = (inLocal: boolean, inServer: boolean): GraphPresence =>
    inLocal && inServer ? "both" : inLocal ? "local" : "server";
  const key = (edge: { fromHash: string; toHash: string }) => `${edge.fromHash}:${edge.toHash}`;
  const localEdges = new Map(local.migrations.map((edge) => [key(edge), edge]));
  const serverEdges = new Map(server.migrations.map((edge) => [key(edge), edge]));
  const migrations = [...new Map([...serverEdges, ...localEdges]).entries()].map(([id, edge]) => ({
    ...edge,
    ...(serverEdges.get(id)?.automatic ? { automatic: true } : {}),
    presence: presence(localEdges.has(id), serverEdges.has(id)),
  }));
  return {
    activeSchemaHash: server.activeSchemaHash,
    localSchemaHash: local.currentSchemaHash,
    schemas,
    schemaPresence: new Map(
      schemas.map((hash) => [hash, presence(localSchemas.has(hash), serverSchemas.has(hash))]),
    ),
    migrations,
  };
}

function presenceLabel(presence?: GraphPresence): string {
  return presence && presence !== "both" ? ` [${presence} only]` : "";
}

export interface MigrationGraphOptions {
  appId: string;
  serverUrl: string;
  adminSecret: string;
}

export async function fetchMigrationGraph(options: MigrationGraphOptions): Promise<MigrationGraph> {
  const response = await fetch(
    appScopedUrl(options.serverUrl, options.appId, "admin/migrations/graph"),
    { headers: { "X-Jazz-Admin-Secret": options.adminSecret } },
  );
  if (!response.ok) {
    const detail = await response.text().catch(() => "");
    throw new Error(
      `Migration graph fetch failed: ${response.status} ${response.statusText}${detail ? ` - ${detail}` : ""}`,
    );
  }
  const graph = (await response.json()) as MigrationGraph;
  const isHash = (hash: unknown): hash is string =>
    typeof hash === "string" && /^[0-9a-f]{64}$/.test(hash);
  if (
    !graph ||
    !Array.isArray(graph.schemas) ||
    !graph.schemas.every(isHash) ||
    new Set(graph.schemas).size !== graph.schemas.length ||
    !(graph.activeSchemaHash === null || graph.schemas.includes(graph.activeSchemaHash)) ||
    !Array.isArray(graph.migrations) ||
    !graph.migrations.every(
      (edge) =>
        edge &&
        graph.schemas.includes(edge.fromHash) &&
        graph.schemas.includes(edge.toHash) &&
        (edge.automatic === undefined || typeof edge.automatic === "boolean"),
    )
  ) {
    throw new Error("Invalid migration graph response from server.");
  }
  return graph;
}

/** Render each edge, expanding each schema once so merges and cycles stay bounded. */
export function renderMigrationGraph(graph: MigrationGraph, color = false): string {
  if (graph.schemas.length === 0) return "No local or server schemas found.";

  const schemas = [...graph.schemas].sort();
  const children = new Map(
    schemas.map((hash) => [
      hash,
      new Map<string, { presence?: GraphPresence; automatic?: boolean }>(),
    ]),
  );
  const incoming = new Set<string>();
  for (const { fromHash, toHash, presence, automatic } of graph.migrations) {
    children.get(fromHash)!.set(toHash, { presence, automatic });
    incoming.add(toHash);
  }
  // Expand colliding prefixes so every displayed schema remains unambiguous.
  let prefixLength = 12;
  while (new Set(schemas.map((hash) => hash.slice(0, prefixLength))).size < schemas.length) {
    prefixLength++;
  }
  const visited = new Set<string>();
  const lines: string[] = [];
  let hasReference = false;
  const roots = schemas.filter((hash) => !incoming.has(hash));
  // Include rootless components too: the server may contain historical cycles.
  for (const root of [...roots, ...schemas]) {
    if (visited.has(root)) continue;
    if (lines.length) lines.push("");
    const stack = [{ hash: root, prefix: "", connector: "", continuation: "" }];
    while (stack.length) {
      const { hash, prefix, connector, continuation } = stack.pop()!;
      const repeated = visited.has(hash);
      const current = hash === graph.activeSchemaHash;
      let label =
        hash.slice(0, prefixLength) +
        (current ? " (current)" : "") +
        (hash === graph.localSchemaHash ? " (schema.ts)" : "") +
        presenceLabel(graph.schemaPresence?.get(hash));
      if (current && color) label = `\x1b[1;36m${label}\x1b[0m`;
      lines.push(`${prefix}${connector}● ${label}${repeated ? " (shown above)" : ""}`);
      if (repeated) {
        hasReference = true;
        continue;
      }
      visited.add(hash);
      const next = [...children.get(hash)!.keys()].sort();
      const edgeLabel = (target: string) => {
        const edge = children.get(hash)!.get(target);
        return edge?.automatic ? " [automatic]" : presenceLabel(edge?.presence);
      };
      const childPrefix = prefix + continuation;
      if (next.length === 1) {
        lines.push(`${childPrefix}│${edgeLabel(next[0]!)}`, `${childPrefix}▼`);
        stack.push({ hash: next[0]!, prefix: childPrefix, connector: "", continuation: "" });
      } else {
        for (let i = next.length - 1; i >= 0; i--) {
          const last = i === next.length - 1;
          stack.push({
            hash: next[i]!,
            prefix: childPrefix,
            connector: (last ? "└─▶" : "├─▶") + edgeLabel(next[i]!) + " ",
            continuation: (last ? "    " : "│   ") + " ".repeat(edgeLabel(next[i]!).length),
          });
        }
      }
    }
  }
  if (hasReference) lines.push("", "Repeated labels refer to the same schema.");
  if (graph.activeSchemaHash === null) lines.push("", "No schema is currently active.");
  return lines.join("\n");
}
