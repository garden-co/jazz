import type { QueryPropagation } from "./client.js";
import { buildEndpointUrl } from "./sync-transport.js";

export interface IntrospectionSubscriptionGroup {
  groupKey: string;
  count: number;
  table: string;
  query: string;
  branches: string[];
  propagation: QueryPropagation;
}

export interface IntrospectionSubscriptionResponse {
  appId: string;
  generatedAt: number;
  queries: IntrospectionSubscriptionGroup[];
}

export interface FetchServerSubscriptionsOptions {
  adminSecret: string;
  appId: string;
  pathPrefix?: string;
}

export async function fetchServerSubscriptions(
  serverUrl: string,
  options: FetchServerSubscriptionsOptions,
): Promise<IntrospectionSubscriptionResponse> {
  const subscriptionsUrl = new URL(
    buildEndpointUrl(serverUrl, "/admin/introspection/subscriptions", options.pathPrefix),
  );
  subscriptionsUrl.searchParams.set("appId", options.appId);

  const response = await fetch(subscriptionsUrl.toString(), {
    method: "GET",
    headers: {
      "X-Jazz-Admin-Secret": options.adminSecret,
    },
  });

  if (!response.ok) {
    const bodyText = await response.text().catch(() => "");
    const detail = bodyText ? ` - ${bodyText}` : "";
    throw new Error(
      `Server subscriptions fetch failed: ${response.status} ${response.statusText}${detail}`,
    );
  }

  const payload = (await response.json()) as Partial<IntrospectionSubscriptionResponse>;
  return {
    appId: typeof payload.appId === "string" ? payload.appId : "",
    generatedAt: typeof payload.generatedAt === "number" ? payload.generatedAt : 0,
    queries: Array.isArray(payload.queries) ? payload.queries : [],
  };
}
