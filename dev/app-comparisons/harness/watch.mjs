// Counts document loads nobody asked for (a tab reloading itself): any
// `domcontentloaded` beyond the one each harness goto/reload causes.
// History-API and hash navigations don't load a document, so aren't counted.
export const reloadsSeen = { count: 0, phases: {} };
export const phase = { name: "setup" };

export function watchBrowser(browser) {
  const origNewContext = browser.newContext.bind(browser);
  browser.newContext = async (...args) => {
    const context = await origNewContext(...args);
    context.on("page", (page) => watchPage(page));
    return context;
  };
}

function watchPage(page) {
  let expected = 0;
  for (const name of ["goto", "reload"]) {
    const orig = page[name].bind(page);
    page[name] = (...args) => {
      expected++;
      return orig(...args);
    };
  }
  page.on("domcontentloaded", () => {
    if (expected > 0) {
      expected--;
      return;
    }
    reloadsSeen.count++;
    reloadsSeen.phases[phase.name] = (reloadsSeen.phases[phase.name] ?? 0) + 1;
    console.error(`  (tab reloaded itself during ${phase.name}: ${page.url()})`);
  });
}
