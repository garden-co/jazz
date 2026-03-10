import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { LiveQueryFilters } from "./LiveQueryFilters";

describe("LiveQueryFilters", () => {
  afterEach(() => {
    cleanup();
  });

  it("renders schema-driven table options and tier options", () => {
    render(
      <LiveQueryFilters
        availableTables={["projects", "todos"]}
        selectedTable=""
        selectedTier=""
        onTableChange={vi.fn()}
        onTierChange={vi.fn()}
      />,
    );

    expect(screen.getByRole("option", { name: "All tables" })).not.toBeNull();
    expect(screen.getByRole("option", { name: "projects" })).not.toBeNull();
    expect(screen.getByRole("option", { name: "todos" })).not.toBeNull();
    expect(screen.getByRole("option", { name: "All tiers" })).not.toBeNull();
    expect(screen.getByRole("option", { name: "worker" })).not.toBeNull();
    expect(screen.getByRole("option", { name: "edge" })).not.toBeNull();
    expect(screen.getByRole("option", { name: "global" })).not.toBeNull();
  });

  it("calls change handlers when filters change", () => {
    const onTableChange = vi.fn();
    const onTierChange = vi.fn();

    render(
      <LiveQueryFilters
        availableTables={["todos"]}
        selectedTable=""
        selectedTier=""
        onTableChange={onTableChange}
        onTierChange={onTierChange}
      />,
    );

    fireEvent.change(screen.getByLabelText("Filter by table"), {
      target: { value: "todos" },
    });
    fireEvent.change(screen.getByLabelText("Filter by tier"), {
      target: { value: "edge" },
    });

    expect(onTableChange).toHaveBeenCalledWith("todos");
    expect(onTierChange).toHaveBeenCalledWith("edge");
  });
});
