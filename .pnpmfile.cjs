module.exports = {
  hooks: {
    readPackage(pkg) {
      if (pkg.name === "@vitest/browser-playwright" && pkg.version === "4.1.0") {
        // Keep shared Vitest providers on the browser revision installed by CI.
        // Docs installs its newer Playwright and Chromium independently.
        pkg.dependencies = { ...pkg.dependencies, playwright: "1.58.2" };
        if (pkg.peerDependencies) delete pkg.peerDependencies.playwright;
      }
      return pkg;
    },
  },
};
