import { NapiDb } from "jazz-napi";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const decode = (value) => Uint8Array.from(Buffer.from(value, "base64"));
const [storage, encodedSchema, encodedConfig] = process.argv.slice(2);
const schema = decode(encodedSchema);
const config = decode(encodedConfig);

const directory =
  storage === "persistent" ? await mkdtemp(join(tmpdir(), "jazz-napi-close-")) : null;
const owner =
  directory === null
    ? NapiDb.openMemory(schema, config)
    : NapiDb.openPersistent(directory, schema, config);
// A registered view intentionally outlives its owner. It shares the core and
// previously retained host callbacks after owner shutdown.
const retainedView = owner.registerSchema(schema);
owner.setTickScheduler(() => undefined);
owner.onMutationError(() => undefined);
await owner.close();
if (directory !== null) await rm(directory, { force: true, recursive: true });
void retainedView;

console.log(`owner closed with ${storage} schema view retained`);
