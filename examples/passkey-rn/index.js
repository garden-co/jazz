import "jazz-tools/react-native/polyfills";
import { Passkey } from "react-native-passkey";
import { setPasskeyModule } from "jazz-tools/react-native-core";

// Inject the passkey module to avoid dynamic require issues in monorepo
setPasskeyModule(Passkey);

import { AppRegistry } from "react-native";
import { name as appName } from "./app.json";
import App from "./src/App";

AppRegistry.registerComponent(appName, () => App);
