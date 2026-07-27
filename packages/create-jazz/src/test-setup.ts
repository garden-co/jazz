// Temporary scaffold commits must not depend on a developer's global signing
// key. This only affects child git processes spawned by the Vitest workers.
process.env.GIT_CONFIG_COUNT = "1";
process.env.GIT_CONFIG_KEY_0 = "commit.gpgsign";
process.env.GIT_CONFIG_VALUE_0 = "false";
