export const TEST_PORT = 19879;
export const APP_ID = "00000000-0000-0000-0000-000000000099";
export const ADMIN_SECRET = "inspector-browser-tests-admin-secret";
export const TEST_ENV = "dev";
export const TEST_BRANCH = "main";
export const SEEDED_TODO_COUNT = Math.max(
  2,
  Number(process.env.JAZZ_INSPECTOR_SEEDED_TODOS ?? "1500"),
);
