if (!import.meta.env.VITE_JAZZ_WORKER_ACCOUNT) {
  throw new Error(".env missing, run `pnpm generate-env`");
}

export const WORKER_ID = import.meta.env.VITE_JAZZ_WORKER_ACCOUNT;
