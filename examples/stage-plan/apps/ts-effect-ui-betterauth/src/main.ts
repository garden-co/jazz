import "@garden-co/design/jazz/tokens.css";
import "@garden-co/design/jazz/fonts.css";
import "./styles.css";
import { BrowserHost } from "@effect/ui/html";
import { Effect, Layer } from "effect";
import { JazzSession } from "jazz-tools/effect-ui";
import { App } from "./App";
import { Auth } from "./auth";

// Better Auth's session drives sign-in; the server's Jazz (`createJazz` in
// server/jazz.ts) reads it in `authenticate`, and the Jazz session follows.
const Main = BrowserHost.mount(App, {}, document.getElementById("app")!).pipe(
  Layer.provide([Auth.layer, JazzSession.layer({ serverUrl: "/jazz" })]),
);

Layer.launch(Main).pipe(Effect.runFork);
