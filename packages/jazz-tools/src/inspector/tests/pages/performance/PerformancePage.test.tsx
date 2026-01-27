// @vitest-environment happy-dom
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { setup } from "goober";
import React from "react";
import { PerformancePage } from "../../../pages/performance/index.js";
import { RouterContext } from "../../../router/context.js";
import type { Router } from "../../../router/context.js";
import { SubscriptionScope } from "jazz-tools";

beforeAll(() => {
  SubscriptionScope.enableProfiling();
  setup(React.createElement);
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

const mockRouter: Router = {
  path: [],
  setPage: vi.fn(),
  addPages: vi.fn(),
  goToIndex: vi.fn(),
  goBack: vi.fn(),
};

function RouterProvider({ children }: { children: React.ReactNode }) {
  return (
    <RouterContext.Provider value={mockRouter}>
      {children}
    </RouterContext.Provider>
  );
}

describe("PerformancePage", () => {
  it("shows empty state when no entries", () => {
    vi.spyOn(performance, "getEntriesByType").mockReturnValue([]);

    render(
      <RouterProvider>
        <PerformancePage onNavigate={() => {}} />
      </RouterProvider>,
    );

    expect(screen.getByText(/No subscriptions recorded yet/)).toBeDefined();
  });

  it("renders a row when performance entries are provided", () => {
    const mockMeasure = {
      entryType: "measure",
      name: "jazz.subscription:test-uuid",
      startTime: 100,
      duration: 50,
      detail: {
        type: "jazz-subscription",
        uuid: "test-uuid",
        id: "co_z123abc",
        source: "useCoState",
        resolve: { depth: 1 },
        status: "loaded",
        startTime: 100,
        endTime: 150,
        duration: 50,
      },
    } as unknown as PerformanceEntry;

    vi.spyOn(performance, "getEntriesByType").mockImplementation((type) => {
      if (type === "measure") return [mockMeasure];
      return [];
    });

    render(
      <RouterProvider>
        <PerformancePage onNavigate={() => {}} />
      </RouterProvider>,
    );

    expect(screen.getByText("co_z123abc")).toBeDefined();
    expect(screen.getByText("useCoState")).toBeDefined();
  });
});
