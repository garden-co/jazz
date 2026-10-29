import { appScopedUrl, fetchAdminJson } from "./url.js";

export interface IntrospectionSubscriptionGroup {
  groupKey: string;
  count: number;
  table: string;
  query: string;
  branches: string[];
}

export interface IntrospectionSubscriptionResponse {
  appId: string;
  generatedAt: number;
  queries: IntrospectionSubscriptionGroup[];
}

export interface FetchServerSubscriptionsOptions {
  adminSecret: string;
  appId: string;
}

export async function fetchServerSubscriptions(
  serverUrl: string,
  options: FetchServerSubscriptionsOptions,
): Promise<IntrospectionSubscriptionResponse> {
  const subscriptionsUrl = new URL(
    appScopedUrl(serverUrl, options.appId, "admin/introspection/subscriptions"),
  );
  subscriptionsUrl.searchParams.set("appId", options.appId);

  const payload = (await fetchAdminJson(
    subscriptionsUrl.toString(),
    options.adminSecret,
    "Server subscriptions",
  )) as Partial<IntrospectionSubscriptionResponse>;
  return {
    appId: typeof payload.appId === "string" ? payload.appId : "",
    generatedAt: typeof payload.generatedAt === "number" ? payload.generatedAt : 0,
    queries: Array.isArray(payload.queries) ? payload.queries : [],
  };
}
