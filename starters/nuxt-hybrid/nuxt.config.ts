import { jazzNuxt } from "jazz-tools/dev/nuxt";

export default defineNuxtConfig({
  vite: {
    plugins: [jazzNuxt()],
  },
  runtimeConfig: {
    public: {
      jazzAppId: "",
      jazzServerUrl: "",
    },
  },
});
