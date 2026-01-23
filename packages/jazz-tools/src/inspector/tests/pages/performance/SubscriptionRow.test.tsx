// @vitest-environment happy-dom
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, fireEvent } from "@testing-library/react";
import { setup } from "goober";
import React from "react";
import { SubscriptionRow } from "../../../pages/performance/SubscriptionRow.js";

beforeAll(() => {
  setup(React.createElement);
});

afterEach(() => {
  cleanup();
});

describe("SubscriptionRow", () => {
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

  it("triggers selection on Enter key", () => {
    const onSelect = vi.fn();
    render(
      <SubscriptionRow
        entry={mockEntry}
        isSelected={false}
        onSelect={onSelect}
        barLeft="0%"
        barWidth="10%"
        barColor="green"
      />,
    );

    fireEvent.keyDown(screen.getByRole("button"), { key: "Enter" });
    expect(onSelect).toHaveBeenCalled();
  });

  it("triggers selection on Space key", () => {
    const onSelect = vi.fn();
    render(
      <SubscriptionRow
        entry={mockEntry}
        isSelected={false}
        onSelect={onSelect}
        barLeft="0%"
        barWidth="10%"
        barColor="green"
      />,
    );

    fireEvent.keyDown(screen.getByRole("button"), { key: " " });
    expect(onSelect).toHaveBeenCalled();
  });

  it("is focusable with tabIndex", () => {
    render(
      <SubscriptionRow
        entry={mockEntry}
        isSelected={false}
        onSelect={() => {}}
        barLeft="0%"
        barWidth="10%"
        barColor="green"
      />,
    );

    expect(screen.getByRole("button").getAttribute("tabindex")).toBe("0");
  });

  it("triggers selection on click", () => {
    const onSelect = vi.fn();
    render(
      <SubscriptionRow
        entry={mockEntry}
        isSelected={false}
        onSelect={onSelect}
        barLeft="0%"
        barWidth="10%"
        barColor="green"
      />,
    );

    fireEvent.click(screen.getByRole("button"));
    expect(onSelect).toHaveBeenCalled();
  });
});
