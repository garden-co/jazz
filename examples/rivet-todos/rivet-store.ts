// Imported only by the server. Rivet credentials never enter the browser bundle.
import { readFile } from "node:fs/promises";
import { rivet, type RivetStoreOptions } from "jazz-tools/backend";

export async function databaseStore(env: Record<string, string>) {
  function required(name: string) {
    const value = env[name];
    if (!value) throw new Error(`${name} is required; see examples/rivet-todos/.env.example`);
    return value;
  }
  const mode = env.RIVET_RUNTIME ?? "existing";
  let runtime: RivetStoreOptions["runtime"];
  if (mode === "existing") {
    runtime = { kind: "existing" };
  } else if (mode === "managed") {
    const image = JSON.parse(await readFile(required("JAZZ_RIVET_IMAGE_JSON"), "utf8"));
    runtime = {
      kind: "managed",
      cloudEndpoint: required("RIVET_CLOUD_ENDPOINT"),
      cloudToken: () => required("RIVET_CLOUD_TOKEN"),
      organization: env.RIVET_ORGANIZATION || undefined,
      cluster: required("RIVET_CLUSTER"),
      region: required("RIVET_REGION"),
      image,
    };
  } else {
    throw new Error("RIVET_RUNTIME must be existing or managed");
  }
  return rivet({
    name: env.JAZZ_RIVET_DATABASE ?? "rivet-todos",
    endpoint: required("RIVET_ENDPOINT"),
    namespace: env.RIVET_NAMESPACE ?? "default",
    token: () => required("RIVET_TOKEN"),
    coreSecret: required("JAZZ_CORE_SECRET"),
    pool: required("RIVET_POOL"),
    runtime,
  });
}
