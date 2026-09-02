import { withJazz } from "jazz-tools/dev/next";

const inMemoryE2E = process.env.JAZZ_E2E_IN_MEMORY === "1";

export default withJazz({}, inMemoryE2E ? { server: { inMemory: true } } : {});
