import { hc } from "hono/client";
import type { AppType } from "./service.js";

export const getWebhookHttpClient = (baseUrl: string) => hc<AppType>(baseUrl);
