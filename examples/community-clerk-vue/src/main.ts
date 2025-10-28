import { clerkPlugin } from "@clerk/vue";
import { createApp } from "vue";
import RootApp from "./components/RootApp.vue";
import "./index.css";

// Import your publishable key
const PUBLISHABLE_KEY = import.meta.env.VITE_CLERK_PUBLISHABLE_KEY;

if (!PUBLISHABLE_KEY) {
  throw new Error("Add your Clerk publishable key to the .env.local file");
}

const app = createApp(RootApp);

app.use(clerkPlugin, {
  publishableKey: PUBLISHABLE_KEY,
  afterSignOutUrl: "/",
});

app.mount("#app");
