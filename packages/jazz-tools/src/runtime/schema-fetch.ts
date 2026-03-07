import type { WasmSchema } from "../drivers/types.js";
import { buildEndpointUrl } from "./sync-transport.js";

export interface FetchStoredWasmSchemaOptions {
  adminSecret: string;
  pathPrefix?: string;
  schemaHash: string;
}

export async function fetchStoredWasmSchema(
  serverUrl: string,
  options: FetchStoredWasmSchemaOptions,
): Promise<{ schema: WasmSchema }> {
  const schemaUrl = buildEndpointUrl(
    serverUrl,
    `/schema/${encodeURIComponent(options.schemaHash)}`,
    options.pathPrefix,
  );

  const response = await fetch(schemaUrl, {
    method: "GET",
    headers: {
      "X-Jazz-Admin-Secret": options.adminSecret,
    },
  });

  if (!response.ok) {
    const bodyText = await response.text().catch(() => "");
    const detail = bodyText ? ` - ${bodyText}` : "";
    throw new Error(`Schema fetch failed: ${response.status} ${response.statusText}${detail}`);
  }

  const schema = (await response.json()) as WasmSchema;
  return { schema };
}

export interface FetchStoredSchemasOptions {
  adminSecret: string;
  pathPrefix?: string;
}

export async function fetchSchemaHashes(
  serverUrl: string,
  options: FetchStoredSchemasOptions,
): Promise<{ hashes: string[] }> {
  const response = await fetch(buildEndpointUrl(serverUrl, "/schemas", options.pathPrefix), {
    method: "GET",
    headers: {
      "X-Jazz-Admin-Secret": options.adminSecret,
    },
  });

  if (!response.ok) {
    const bodyText = await response.text().catch(() => "");
    const detail = bodyText ? ` - ${bodyText}` : "";
    throw new Error(
      `Schema hashes fetch failed: ${response.status} ${response.statusText}${detail}`,
    );
  }

  const schemaHashesResponse = (await response.json()) as { hashes?: string[] };
  return { hashes: schemaHashesResponse.hashes ?? [] };
}
