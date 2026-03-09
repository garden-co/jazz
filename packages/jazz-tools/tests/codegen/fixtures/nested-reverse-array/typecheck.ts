import { app } from "./app.ts";

declare function read<T>(_value: T): void;

const nested = app.resources.include({
  resource_access_edgesViaResource: { team: true },
});

declare const row: typeof nested._rowType;
read(row.resource_access_edgesViaResource?.[0].team?.id);
