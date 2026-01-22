import type { Page } from "@playwright/test";
import type { ScenarioDefinition, ScenarioResult } from "./types.ts";

/**
 * Grid scenario configuration options
 */
export interface GridScenarioOptions {
  /** Grid size NxN */
  size: number;
  /** Minimum padding bytes per cell */
  minPadding: number;
  /** Maximum padding bytes per cell */
  maxPadding: number;
}

/**
 * Default options for grid scenario
 */
export const defaultGridOptions: GridScenarioOptions = {
  size: 10,
  minPadding: 0,
  maxPadding: 100,
};

/**
 * Grid scenario definition
 */
export const gridScenario: ScenarioDefinition = {
  name: "grid",

  async generate(page: Page, config: Record<string, unknown>): Promise<string> {
    const options = {
      ...defaultGridOptions,
      ...config,
    } as GridScenarioOptions;

    // Navigate to grid home
    await page.goto("/grid");

    // Fill in the form
    await page.locator("#size").fill(String(options.size));
    await page.locator("#minPadding").fill(String(options.minPadding));
    await page.locator("#maxPadding").fill(String(options.maxPadding));

    // Click generate button and wait for it to complete
    await page.getByRole("button", { name: "Generate Grid" }).click();

    // Wait for the button to show "Generate Grid" again (not "Generating...")
    await page.getByRole("button", { name: "Generate Grid" }).waitFor({
      state: "visible",
    });

    // Wait a bit for the grid to appear in the list
    await page.waitForTimeout(500);

    // Find the newly created grid and click on it
    const gridCard = page
      .locator(`text=${options.size}x${options.size}`)
      .first();
    await gridCard.click();

    // Wait for navigation to the grid screen
    await page.waitForURL(/\/grid\/co_z/);

    // Extract the CoValue ID from the URL
    const url = page.url();
    const match = url.match(/\/grid\/(co_z[a-zA-Z0-9]+)/);
    if (!match) {
      throw new Error(`Could not extract grid CoValue ID from URL: ${url}`);
    }

    return match[1];
  },

  async run(page: Page, coValueId: string): Promise<ScenarioResult> {
    // Navigate directly to the grid
    await page.goto(`/grid/${coValueId}`);

    // Wait for the load time element to have the data attribute set
    const loadTimeElement = page.locator('[data-testid="load-time"]');

    // Wait for the element to have a non-null data-load-time-ms attribute
    await loadTimeElement.waitFor({ state: "visible" });

    // Poll until the attribute is set (not null)
    const loadTimeMs = await page.waitForFunction(() => {
      const el = document.querySelector('[data-testid="load-time"]');
      if (!el) return null;
      const value = el.getAttribute("data-load-time-ms");
      if (value === null) return null;
      return parseFloat(value);
    });

    const value = await loadTimeMs.jsonValue();
    if (value === null) {
      throw new Error("Failed to get load time metric");
    }

    return {
      metrics: {
        loadTimeMs: value,
      },
    };
  },
};
