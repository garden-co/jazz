import { hc } from "hono/client";
import type { AppType } from "./service";

export const getWebhookHttpClient = (baseUrl: string) => hc<AppType>(baseUrl);
