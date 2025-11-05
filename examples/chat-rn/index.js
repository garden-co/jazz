import "./src/polyfills";
import { AppRegistry } from "react-native";
import { name as appName } from "./app.json";
import App from "./src/App";
import { uniffiInitAsync } from "rn-cojson-core-spec";

uniffiInitAsync().then(() => {
  AppRegistry.registerComponent(appName, () => App);
});
