import { it } from "vitest";
import { runOfflineDeliveryRetry } from "./fixtures/offline-delivery-retry.js";

it.each([["interrupted background read", "ordinary owner read"]] as const)(
  "resumes recipient delivery after failed administration (%s, %s)",
  runOfflineDeliveryRetry,
  60_000,
);
