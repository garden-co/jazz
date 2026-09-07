import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createRequire } from "node:module";
import { join } from "node:path";
import { build } from "esbuild";
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import ts from "typescript";
import { compile, compileModule } from "svelte/compiler";

const packageDir =
  process.env.JAZZ_CLASSIC_CONSUMER_DIR ?? fileURLToPath(new URL("..", import.meta.url));

function runConsumer(source, { browser = false } = {}) {
  const args = [];
  if (browser) {
    const require = createRequire(join(packageDir, "package.json"));
    const setup = `
      import { JSDOM } from ${JSON.stringify(pathToFileURL(require.resolve("jsdom")).href)};
      const dom = new JSDOM('<div id="app"></div>', { url: "https://jazz.test", pretendToBeVisual: true });
      for (const name of [
        "window", "document", "navigator", "HTMLElement", "Element", "Node", "Text", "Comment",
        "Event", "CustomEvent", "SVGElement", "MutationObserver",
      ]) Object.defineProperty(globalThis, name, { value: dom.window[name], configurable: true });
      globalThis.requestAnimationFrame = dom.window.requestAnimationFrame.bind(dom.window);
      globalThis.cancelAnimationFrame = dom.window.cancelAnimationFrame.bind(dom.window);
    `;
    args.push(
      "--conditions=browser",
      "--import",
      "data:text/javascript," + encodeURIComponent(setup),
    );
  }
  args.push("--input-type=module");
  const result = spawnSync(process.execPath, args, {
    cwd: packageDir,
    input: source,
    encoding: "utf8",
    timeout: 30_000,
  });
  assert.equal(result.status, 0, result.error?.message ?? result.stderr);
}

async function runFrameworkConsumer(source, { browser = false, controlledFactory = false } = {}) {
  const require = createRequire(join(packageDir, "package.json"));
  const manifest = require("jazz-tools/package.json");
  const generate = browser ? "client" : "server";
  const result = await build({
    stdin: {
      contents: compileModule(source, { filename: "consumer.svelte.js", generate }).js.code,
      resolveDir: packageDir,
    },
    bundle: true,
    platform: "node",
    format: "esm",
    conditions: browser ? ["browser"] : ["node"],
    external: Object.keys({
      ...manifest.dependencies,
      ...manifest.devDependencies,
      ...manifest.peerDependencies,
    }),
    write: false,
    plugins: [
      {
        name: "compiled-public-svelte",
        setup(build) {
          // Race tests control only the existing client-factory dependency. The
          // public provider, renderer and scheduler still execute their real code.
          if (controlledFactory) {
            build.onLoad(
              { filter: /[\\/]dist[\\/](svelte|vue)[\\/]create-jazz-client\.js$/ },
              () => ({
                contents:
                  "export const createJazzClient = (...args) => globalThis.__classicClientFactory(...args);",
                loader: "js",
              }),
            );
          }
          build.onLoad({ filter: /\.svelte(\.js)?$/ }, ({ path }) => ({
            contents: (path.endsWith(".svelte") ? compile : compileModule)(
              readFileSync(path, "utf8"),
              { filename: path, generate },
            ).js.code,
            loader: "js",
          }));
        },
      },
    ],
  });
  runConsumer(result.outputFiles[0].text, { browser });
}

test("Classic co and z named imports lead to migration guidance, not a linking error", () => {
  runConsumer(`
    import assert from "node:assert/strict";
    import { co, z } from "jazz-tools";

    assert.throws(() => co.map({ title: z.string() }), error => {
      assert.equal(error.code, "JAZZ_CLASSIC_API_REMOVED");
      assert.match(error.message, /co.map/);
      assert.match(error.message, /Jazz Classic/);
      assert.match(error.message, /Jazz 2/);
      assert.ok(error.message.includes("node_modules/jazz-tools/README.md"));
      assert.ok(error.message.includes("https://jazz.tools/llms-full.txt"));
      return true;
    });
  `);
});

test("Classic namespaces and classes reject reads, calls, construction and subclassing", () => {
  runConsumer(`
    import assert from "node:assert/strict";
    import * as jazz from "jazz-tools";
    const check = (operation, name) => assert.throws(operation, error =>
      error.code === "JAZZ_CLASSIC_API_REMOVED" && error.message.includes(name));

    for (const name of [
      "co", "z", "CoMap", "CoList", "CoFeed", "CoPlainText", "CoRichText",
      "FileStream", "Account", "Group", "Profile",
    ]) {
      check(() => jazz[name].create, name + ".create");
    }
    check(() => jazz.z.string(), "z.string");
    check(() => jazz.Group.create(), "Group.create");
    check(() => jazz.CoMap(), "CoMap");
    check(() => new jazz.CoMap(), "CoMap");
    check(() => class Task extends jazz.CoMap {}, "CoMap");
  `);
});

test("Next Fast Refresh can inspect unused Classic exports without treating them as components", () => {
  runConsumer(`
    import assert from "node:assert/strict";
    import { createRequire } from "node:module";
    import * as jazz from "jazz-tools";
    // Fast Refresh is development-only, even when the outer suite tests production.
    process.env.NODE_ENV = "development";
    const require = createRequire(import.meta.url);
    const refresh = require("next/dist/compiled/react-refresh/runtime");
    const helpers = require("next/dist/compiled/@next/react-refresh-utils/dist/internal/helpers").default;
    helpers.registerExportsForReactRefresh(jazz, "classic-diagnostic-consumer");
    for (const name of [
      "co", "z", "CoMap", "CoList", "CoFeed", "CoPlainText", "CoRichText",
      "FileStream", "Account", "Group", "Profile",
    ]) {
      assert.equal(refresh.isLikelyComponentType(jazz[name]), false, name);
    }
    assert.equal(helpers.isReactRefreshBoundary(jazz), false);
  `);
});

for (const [entrypoint, provider] of [
  ["react", "JazzReactProvider"],
  ["react-core", null],
  ["react-native", "JazzReactNativeProvider"],
  ["expo", "JazzExpoProvider"],
]) {
  test(`Classic ${entrypoint} hooks fail directly and providers fail only when rendered`, () => {
    // Expo's public barrel needs native peers. Substitute only those host modules,
    // and fail if used; this is export/render coverage, not Expo device coverage.
    const expoHostAdapters = `
      import { register } from "node:module";
      const host = "data:text/javascript," + encodeURIComponent(\`
        const unavailable = () => { throw new Error("Unexpected Expo host call"); };
        export {
          unavailable as getRandomBytes, unavailable as getItemAsync,
          unavailable as setItemAsync, unavailable as deleteItemAsync,
        };
      \`);
      register("data:text/javascript," + encodeURIComponent(\`
        export function resolve(specifier, context, nextResolve) {
          if (specifier === "expo-crypto" || specifier === "expo-secure-store") {
            return { url: \${JSON.stringify(host)}, shortCircuit: true };
          }
          return nextResolve(specifier, context);
        }
      \`), import.meta.url);
    `;
    runConsumer(`
      import assert from "node:assert/strict";
      import { createElement, isValidElement } from "react";
      import { renderToString } from "react-dom/server";
      ${entrypoint === "expo" ? expoHostAdapters : ""}
      const binding = await import("jazz-tools/${entrypoint}");
      for (const name of ["useCoState", "useAccount", "useSuspenseCoState", "useSuspenseAccount"]) {
        assert.throws(() => binding[name](), error =>
          error.code === "JAZZ_CLASSIC_API_REMOVED" && error.message.includes(name));
      }
      const provider = ${JSON.stringify(provider)};
      if (provider) {
        const element = createElement(binding[provider], {}, "child");
        assert.ok(isValidElement(element));
        assert.throws(() => renderToString(element), error =>
          error.code === "JAZZ_CLASSIC_API_REMOVED" && error.message.includes(provider));
      }
      if (${JSON.stringify(entrypoint)} === "expo") {
        function CurrentAuth() {
          const auth = binding.useLocalFirstAuth({ appId: "classic-coexistence" });
          return createElement("p", {}, auth.isLoading ? "loading" : "ready");
        }
        assert.equal(renderToString(createElement(CurrentAuth)), "<p>loading</p>");
      } else {
        const current = createElement(binding.JazzProvider, {
          config: { appId: "classic-coexistence" },
          fallback: createElement("p", {}, "loading"),
          autoAttachDevTools: false,
          ...(${JSON.stringify(entrypoint)} === "react-core" ? { createJazzClient: binding.createJazzClient } : {}),
        }, "ready");
        assert.equal(renderToString(current), "<p>loading</p>");
      }
    `);
  });
}

test("ordinary production tree shaking preserves Classic failure side effects", async () => {
  const require = createRequire(join(packageDir, "package.json"));
  for (const operation of ["co.map;", "co.map({});", "new CoMap();"]) {
    const result = await build({
      stdin: {
        contents: `
          import assert from "node:assert/strict";
          import { co, CoMap } from ${JSON.stringify(require.resolve("jazz-tools"))};
          assert.throws(() => { ${operation} }, { code: "JAZZ_CLASSIC_API_REMOVED" });
        `,
        resolveDir: packageDir,
      },
      bundle: true,
      minify: true,
      treeShaking: true,
      packages: "external",
      platform: "node",
      format: "esm",
      write: false,
    });
    runConsumer(result.outputFiles[0].text);
  }
});

for (const [file, bundler] of [
  ["classic-api.typecheck.tsx", false],
  ["classic-svelte-api.typecheck.ts", true],
]) {
  test(`published ${bundler ? "Svelte" : "Node"} declarations reject Classic use and accept Jazz 2`, () => {
    // Parallel CI consumers verify source identity; scratch inputs must be ignored.
    const scratchDir = join(packageDir, ".test-tmp");
    mkdirSync(scratchDir, { recursive: true });
    const fixtureDir = mkdtempSync(join(scratchDir, "classic-api-types-"));
    const fixture = join(fixtureDir, "consumer.tsx");
    try {
      copyFileSync(new URL("../tests/public-api/" + file, import.meta.url), fixture);
      const program = ts.createProgram([fixture], {
        noEmit: true,
        strict: true,
        skipLibCheck: true,
        target: ts.ScriptTarget.ES2022,
        module: bundler ? ts.ModuleKind.ESNext : ts.ModuleKind.NodeNext,
        moduleResolution: bundler
          ? ts.ModuleResolutionKind.Bundler
          : ts.ModuleResolutionKind.NodeNext,
        jsx: ts.JsxEmit.ReactJSX,
        esModuleInterop: true,
        types: ["node", "react"],
      });
      const diagnostics = ts.getPreEmitDiagnostics(program);
      assert.equal(
        diagnostics.length,
        0,
        ts.formatDiagnosticsWithColorAndContext(diagnostics, {
          getCanonicalFileName: (file) => file,
          getCurrentDirectory: () => packageDir,
          getNewLine: () => "\n",
        }),
      );
    } finally {
      rmSync(fixtureDir, { recursive: true, force: true });
    }
  });
}

test("Vue rejects Classic provider configuration instead of silently ignoring it", () => {
  runConsumer(`
    import assert from "node:assert/strict";
    import { createSSRApp, h } from "vue";
    import { renderToString } from "@vue/server-renderer";
    import { JazzProvider } from "jazz-tools/vue";

    const app = createSSRApp({
      render: () => h(JazzProvider, {
        config: { appId: "classic-vue-props" },
        sync: { peer: "wss://classic.invalid" },
      }, {
        fallback: () => h("p", "invalid-fallback"),
        default: () => h("p", "invalid-child"),
      }),
    });
    const failures = [];
    app.config.errorHandler = error => failures.push(error);
    const html = await renderToString(app);
    assert.equal(failures.length, 1);
    assert.equal(failures[0].code, "JAZZ_CLASSIC_API_REMOVED");
    assert.ok(failures[0].message.includes("JazzProvider.sync"));
    assert.ok(!html.includes("invalid-fallback") && !html.includes("invalid-child"));
    const valid = await renderToString(createSSRApp({
      render: () => h(JazzProvider, { config: { appId: "current-vue-props" } },
        { fallback: () => h("p", "valid-fallback") }),
    }));
    assert.ok(valid.includes("<p>valid-fallback</p>"));
  `);
});

test("Svelte Classic classes and Vue Classic composables reject through public exports", async () => {
  await runFrameworkConsumer(`
    import assert from "node:assert/strict";
    import { CoState, AccountCoState, InviteListener, SyncConnectionStatus } from "jazz-tools/svelte";
    import { useCoState, useAccount, useAccountOrGuest, useJazzContext, useAcceptInvite } from "jazz-tools/vue";
    for (const [name, value] of Object.entries({ CoState, AccountCoState, InviteListener, SyncConnectionStatus })) {
      assert.throws(() => new value(), error =>
        error.code === "JAZZ_CLASSIC_API_REMOVED" && error.message.includes(name));
    }
    for (const [name, value] of Object.entries({ useCoState, useAccount, useAccountOrGuest, useJazzContext, useAcceptInvite })) {
      assert.throws(() => value(), error =>
        error.code === "JAZZ_CLASSIC_API_REMOVED" && error.message.includes(name));
    }
  `);
});

test("reused Svelte and Vue providers reject present Classic props but preserve valid SSR", async () => {
  await runFrameworkConsumer(`
    import assert from "node:assert/strict";
    import { createRawSnippet } from "svelte";
    import { render } from "svelte/server";
    import { JazzSvelteProvider } from "jazz-tools/svelte";
    import { createSSRApp, h } from "vue";
    import { renderToString } from "@vue/server-renderer";
    import { JazzProvider } from "jazz-tools/vue";
    const config = { appId: "classic-props-presence" };
    const fallback = createRawSnippet(() => ({ render: () => "<p>loading</p>" }));
    for (const legacy of [{sync: undefined}, {AccountSchema: class Account {}}, {"account-schema": undefined}]) {
      assert.throws(() => render(JazzSvelteProvider, {
        props: { config, children: fallback, fallback, ...legacy },
      }).body, error => error.code === "JAZZ_CLASSIC_API_REMOVED" &&
        error.message.includes("JazzSvelteProvider." + Object.keys(legacy)[0]));
      const app = createSSRApp({
        render: () => h(JazzProvider, { config, ...legacy }, {
          fallback: () => h("p", "invalid-fallback"), default: () => h("p", "invalid-child"),
        }),
      });
      const failures = [];
      app.config.errorHandler = error => failures.push(error);
      const html = await renderToString(app);
      assert.equal(failures.length, 1);
      assert.equal(failures[0].code, "JAZZ_CLASSIC_API_REMOVED");
      assert.ok(failures[0].message.includes("JazzProvider." + Object.keys(legacy)[0]));
      assert.ok(!html.includes("invalid-fallback") && !html.includes("invalid-child"));
    }
    const svelte = render(JazzSvelteProvider, {
      props: { config, children: fallback, fallback, "data-extra": "allowed" },
    });
    assert.ok(svelte.body.includes("<p>loading</p>"));
    const vue = await renderToString(createSSRApp({
      render: () => h(JazzProvider, { config, "data-extra": "allowed" }, { fallback: () => h("p", "loading") }),
    }));
    assert.ok(vue.includes("<p>loading</p>"));
  `);
});

test("Svelte notices newly introduced Classic props even when config is unchanged", async () => {
  await runFrameworkConsumer(
    `
    import assert from "node:assert/strict";
    import { createRawSnippet, mount, unmount, flushSync } from "svelte";
    import { JazzSvelteProvider } from "jazz-tools/svelte";
    const snippet = createRawSnippet(() => ({ render: () => "<p>loading</p>" }));
    const props = $state({
      config: { appId: "svelte-prop-transition", driver: { type: "memory" } },
      children: snippet, fallback: snippet,
    });
    const instance = mount(JazzSvelteProvider, { target: document.getElementById("app"), props });
    flushSync();
    assert.equal(document.querySelector("p").textContent, "loading");
    props.sync = undefined;
    assert.throws(() => flushSync(), error =>
      error.code === "JAZZ_CLASSIC_API_REMOVED" && error.message.includes("JazzSvelteProvider.sync"));
    await unmount(instance);
  `,
    { browser: true },
  );
});

test("Vue notices newly introduced Classic attrs even when config is unchanged", async () => {
  await runFrameworkConsumer(
    `
    import assert from "node:assert/strict";
    import { createApp, h, reactive, nextTick } from "vue";
    import { JazzProvider } from "jazz-tools/vue";
    const props = reactive({ config: { appId: "vue-prop-transition", driver: { type: "memory" } } });
    const failures = [];
    const app = createApp({
      render: () => h(JazzProvider, props, { fallback: () => h("p", "loading") }),
    });
    app.config.errorHandler = error => failures.push(error);
    app.mount(document.getElementById("app"));
    assert.equal(document.querySelector("p").textContent, "loading");
    props.AccountSchema = undefined;
    await nextTick();
    assert.ok(failures.some(error => error.code === "JAZZ_CLASSIC_API_REMOVED" &&
      error.message.includes("JazzProvider.AccountSchema")));
    app.unmount();
  `,
    { browser: true },
  );
});

test("Solid's supported provider still renders through its Node export", async () => {
  await runFrameworkConsumer(`
    import assert from "node:assert/strict";
    import { createComponent } from "solid-js";
    import { renderToString } from "solid-js/web";
    import { JazzProvider } from "jazz-tools/solid";
    const html = renderToString(() => createComponent(JazzProvider, {
      config: { appId: "solid-coexistence" },
      fallback: "loading", children: "ready", autoAttachDevTools: false,
    }));
    assert.ok(html.includes("loading"));
  `);
});

test("Solid's supported provider still mounts through its browser export", async () => {
  await runFrameworkConsumer(
    `
    import assert from "node:assert/strict";
    import { createComponent } from "solid-js";
    import { render } from "solid-js/web";
    import { JazzProvider } from "jazz-tools/solid";
    const target = document.getElementById("app");
    const dispose = render(() => createComponent(JazzProvider, {
      config: { appId: "solid-coexistence", driver: { type: "memory" } },
      fallback: "loading", children: "ready", autoAttachDevTools: false,
    }), target);
    assert.equal(target.textContent, "loading");
    dispose();
  `,
    { browser: true },
  );
});

test("Vue refuses replacement when Classic attrs arrive during prior-client shutdown", async () => {
  await runFrameworkConsumer(
    `
    import assert from "node:assert/strict";
    import { setImmediate } from "node:timers/promises";
    import { createApp, h, reactive, nextTick } from "vue";
    import { JazzProvider } from "jazz-tools/vue";
    let finishShutdown, startedShutdown;
    const stopping = new Promise(resolve => { startedShutdown = resolve; });
    const stopped = new Promise(resolve => { finishShutdown = resolve; });
    const resources = [];
    globalThis.__classicClientFactory = async () => {
      const resource = { closed: false };
      resources.push(resource);
      return {
        db: { onAuthChanged: () => () => {} }, session: null,
        async shutdown() { startedShutdown(); await stopped; resource.closed = true; },
      };
    };
    const props = reactive({ config: { appId: "first" }, autoAttachDevTools: false });
    const failures = [];
    const app = createApp({ render: () => h(JazzProvider, props, {
      default: () => h("p", "ready"), fallback: () => h("p", "loading"),
    }) });
    app.config.errorHandler = error => failures.push(error);
    app.mount(document.getElementById("app"));
    await setImmediate();
    assert.equal(document.querySelector("p").textContent, "ready");
    props.config = { appId: "second" };
    await stopping;
    props.sync = undefined;
    await nextTick();
    finishShutdown();
    await setImmediate();
    assert.equal(resources.length, 1, "invalid replacement must not acquire another resource");
    assert.equal(resources[0].closed, true);
    assert.ok(failures.some(error => error.code === "JAZZ_CLASSIC_API_REMOVED"));
    app.unmount();
  `,
    { browser: true, controlledFactory: true },
  );
});

for (const framework of ["svelte", "vue"]) {
  test(`${framework} disposes an in-flight client invalidated before publication`, async () => {
    await runFrameworkConsumer(
      `
      import assert from "node:assert/strict";
      import { setImmediate } from "node:timers/promises";
      import { mount, unmount, flushSync, createRawSnippet } from "svelte";
      import { JazzSvelteProvider } from "jazz-tools/svelte";
      import { createApp, h, reactive, nextTick } from "vue";
      import { JazzProvider } from "jazz-tools/vue";
      let finishCreation, startedCreation;
      const started = new Promise(resolve => { startedCreation = resolve; });
      const pending = new Promise(resolve => { finishCreation = resolve; });
      let closed = false;
      const client = {
        db: {
          onAuthChanged: () => () => {},
          read() { if (closed) throw new Error("resource closed"); return "usable"; },
        },
        session: null,
        async shutdown() { closed = true; },
      };
      globalThis.__classicClientFactory = () => { startedCreation(); return pending; };
      const failures = [];
      const capture = error => {
        if (error.code !== "JAZZ_CLASSIC_API_REMOVED") throw error;
        failures.push(error);
      };
      const target = document.getElementById("app");
      const config = { appId: "pending-create" };
      let props, dispose;
      if (${JSON.stringify(framework)} === "svelte") {
        // Non-reactive caller props force the post-await admission check, rather
        // than letting an effect rerun cancel creation on our behalf.
        props = { config, autoAttachDevTools: false,
          children: createRawSnippet(() => ({ render: () => "<p>ready</p>" })),
          fallback: createRawSnippet(() => ({ render: () => "<p>loading</p>" })),
        };
        process.on("uncaughtException", capture);
        const instance = mount(JazzSvelteProvider, { target, props });
        flushSync();
        dispose = () => unmount(instance);
      } else {
        props = reactive({ config, autoAttachDevTools: false });
        const app = createApp({ render: () => h(JazzProvider, props, {
          default: () => h("p", "ready"), fallback: () => h("p", "loading"),
        }) });
        app.config.errorHandler = capture;
        app.mount(target);
        dispose = () => app.unmount();
      }
      await started;
      props.AccountSchema = undefined;
      await nextTick();
      finishCreation(client);
      await setImmediate();
      assert.throws(() => client.db.read(), /resource closed/);
      assert.ok(!target.textContent.includes("ready"), "rejected resource must not reach children");
      assert.ok(failures.some(error => error.code === "JAZZ_CLASSIC_API_REMOVED"));
      await dispose();
    `,
      { browser: true, controlledFactory: true },
    );
  });
}
