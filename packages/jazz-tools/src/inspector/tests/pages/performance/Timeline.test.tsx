// @vitest-environment happy-dom
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, fireEvent } from "@testing-library/react";
import { setup } from "goober";
import React from "react";
import { Timeline } from "../../../pages/performance/Timeline.js";

beforeAll(() => {
  setup(React.createElement);
});

afterEach(() => {
  cleanup();
});

describe("Timeline", () => {
  it("shows clear selection button when selection exists", () => {
    render(
      <Timeline
        entries={[]}
        timeRange={{ min: 0, max: 1000 }}
        selection={[100, 500]}
        onSelectionChange={() => {}}
      />,
    );

    expect(screen.getByText("Clear selection")).toBeDefined();
  });

  it("calls onSelectionChange(null) when clear button clicked", () => {
    const onSelectionChange = vi.fn();
    render(
      <Timeline
        entries={[]}
        timeRange={{ min: 0, max: 1000 }}
        selection={[100, 500]}
        onSelectionChange={onSelectionChange}
      />,
    );

    fireEvent.click(screen.getByText("Clear selection"));
    expect(onSelectionChange).toHaveBeenCalledWith(null);
  });

  it("does not show clear selection button when no selection", () => {
    render(
      <Timeline
        entries={[]}
        timeRange={{ min: 0, max: 1000 }}
        selection={null}
        onSelectionChange={() => {}}
      />,
    );

    expect(screen.queryByText("Clear selection")).toBeNull();
  });
});
