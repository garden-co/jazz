import { render } from "solid-js/web";
import { App } from "./App.js";

render(
  () => (
    <App
      config={{
        env: "dev",
      }}
    />
  ),
  document.getElementById("app")!,
);
