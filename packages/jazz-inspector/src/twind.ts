import { install, tw } from "@twind/core";
import config from "./twind.config";

// Install Twind globally
install(config);

// Export the tw function for use in components
export { tw };
