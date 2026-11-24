import {
  createJazzCore,
  routeJazzRequest,
  s3,
  vercelBlob,
  type JazzCoreEnv,
} from "jazz-tools/durable-object";

interface Env extends JazzCoreEnv {
  TEST_BLOBS?: string;
}

export const JazzCore = createJazzCore<Env>({
  blobs: (env) => {
    if (env.TEST_BLOBS === "s3") return s3({ bucket: "app-chunks", region: "us-east-1" });
    if (env.TEST_BLOBS === "vercel-blob") return vercelBlob();
    return "sqlite";
  },
});
export default { fetch: routeJazzRequest };
