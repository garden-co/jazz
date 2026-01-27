// @vitest-environment happy-dom
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, fireEvent } from "@testing-library/react";
import { setup } from "goober";
import React from "react";
import { SubscriptionDetailPanel } from "../../../pages/performance/SubscriptionDetailPanel.js";

beforeAll(() => {
  setup(React.createElement);
});

afterEach(() => {
  cleanup();
});

describe("SubscriptionDetailPanel", () => {
  const mockEntry = {
    uuid: "test-uuid",
    id: "co_z123",
    source: "useCoState",
    resolve: "{}",
    status: "loaded" as const,
    startTime: 0,
    endTime: 100,
    duration: 100,
  };

  it("calls onClose when close button is clicked", () => {
    const onClose = vi.fn();
    render(
      <SubscriptionDetailPanel
        entry={mockEntry}
        onNavigate={() => {}}
        onClose={onClose}
      />,
    );

    fireEvent.click(screen.getByLabelText("Close detail panel"));
    expect(onClose).toHaveBeenCalled();
  });

  it("calls onNavigate when CoValue link is clicked", () => {
    const onNavigate = vi.fn();
    render(
      <SubscriptionDetailPanel
        entry={mockEntry}
        onNavigate={onNavigate}
        onClose={() => {}}
      />,
    );

    fireEvent.click(screen.getByText("co_z123"));
    expect(onNavigate).toHaveBeenCalledWith("co_z123");
  });

  it("displays entry details", () => {
    render(
      <SubscriptionDetailPanel
        entry={mockEntry}
        onNavigate={() => {}}
        onClose={() => {}}
      />,
    );

    expect(screen.getByText("useCoState")).toBeDefined();
    expect(screen.getByText("co_z123")).toBeDefined();
  });
});
