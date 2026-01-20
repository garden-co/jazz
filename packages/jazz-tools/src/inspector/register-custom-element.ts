import { SubscriptionScope } from "jazz-tools";

if (typeof window !== "undefined" && process.env.NODE_ENV === "development") {
  SubscriptionScope.enableProfiling();
  import("./custom-element.js");
}
