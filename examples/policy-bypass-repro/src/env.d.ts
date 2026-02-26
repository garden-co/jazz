/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_JAZZ_SERVER_URL?: string;
  readonly VITE_JAZZ_APP_ID?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
