import { app } from "./app.ts";

const nested = app.resources.include({
  resource_access_edgesViaResource: { team: true },
});

declare const row: typeof nested._rowType;
row.resource_access_edgesViaResource?.[0].team?.id;
