// @vitest-environment happy-dom
import { describe, it, expect, assert, beforeAll, afterEach } from "vitest";
import { render, screen, fireEvent, cleanup } from "@testing-library/react";
import { DataTable, ColumnDef } from "../../ui/data-table";
import { setup } from "goober";
import React from "react";

type TestItem = {
  id: number;
  name: string;
  value: number;
};

const mockData: TestItem[] = [
  { id: 1, name: "Apple", value: 100 },
  { id: 2, name: "Banana", value: 50 },
  { id: 3, name: "Cherry", value: 200 },
  { id: 4, name: "Date", value: 75 },
  { id: 5, name: "Elderberry", value: 150 },
];

const mockColumns: ColumnDef<TestItem>[] = [
  {
    id: "name",
    header: "Name",
    accessor: (item) => item.name,
    sortable: true,
    filterable: true,
  },
  {
    id: "value",
    header: "Value",
    accessor: (item) => item.value.toString(),
    sortable: true,
    sortFn: (a, b) => a.value - b.value,
  },
];

describe("DataTable", () => {
  beforeAll(() => {
    // setup goober
    setup(React.createElement);
  });

  afterEach(() => {
    cleanup();
  });

  it("renders table with data", () => {
    render(
      <DataTable
        columns={mockColumns}
        data={mockData}
        pageSize={10}
        getRowKey={(item) => item.id.toString()}
      />,
    );

    expect(screen.getByText("Apple")).toBeDefined();
    expect(screen.getByText("Banana")).toBeDefined();
    expect(screen.getByText("Cherry")).toBeDefined();
  });

  it("shows empty message when no data", () => {
    render(
      <DataTable
        columns={mockColumns}
        data={[]}
        pageSize={10}
        getRowKey={(item) => item.id.toString()}
        emptyMessage="No items found"
      />,
    );

    expect(screen.getByText("No items found")).toBeDefined();
  });

  it("hides pagination when data fits on one page", () => {
    render(
      <DataTable
        columns={mockColumns}
        data={mockData.slice(0, 3)}
        pageSize={10}
        getRowKey={(item) => item.id.toString()}
      />,
    );

    expect(screen.queryByText(/Page \d+ of \d+/)).toBeNull();
  });

  it("shows pagination when data exceeds page size", () => {
    const largeData = Array.from({ length: 25 }, (_, i) => ({
      id: i,
      name: `Item ${i}`,
      value: i * 10,
    }));

    render(
      <DataTable
        columns={mockColumns}
        data={largeData}
        pageSize={10}
        getRowKey={(item) => item.id.toString()}
      />,
    );

    expect(screen.getByText(/Page 1 of 3/)).toBeDefined();
  });

  it("supports sorting when clicking header", () => {
    render(
      <DataTable
        columns={mockColumns}
        data={mockData}
        pageSize={10}
        getRowKey={(item) => item.id.toString()}
      />,
    );

    const nameHeader = screen.getByText("Name").parentElement;

    // Click to sort ascending
    fireEvent.click(nameHeader!);
    const rows = screen.getAllByRole("row");
    expect(rows[2]?.textContent).toContain("Apple"); // First data row

    // Click to sort descending
    fireEvent.click(nameHeader!);
    const rowsDesc = screen.getAllByRole("row");
    expect(rowsDesc[2]?.textContent).toContain("Elderberry");
  });

  it("applies initial sort configuration", () => {
    render(
      <DataTable
        columns={mockColumns}
        data={mockData}
        pageSize={10}
        initialSort={{ columnId: "value", direction: "desc" }}
        getRowKey={(item) => item.id.toString()}
      />,
    );

    const rows = screen.getAllByRole("row");
    expect(rows[2]?.textContent).toContain("Cherry"); // Highest value (200)
  });

  it("filters data based on input", () => {
    render(
      <DataTable
        columns={mockColumns}
        data={mockData}
        pageSize={10}
        getRowKey={(item) => item.id.toString()}
      />,
    );

    const filterInput = screen.getAllByPlaceholderText("Filter name")[0];
    assert(filterInput);
    fireEvent.change(filterInput, { target: { value: "berry" } });

    expect(screen.getByText("Elderberry")).toBeDefined();
    expect(screen.queryByText("Apple")).toBeNull();
  });

  it("shows filtered count in pagination info", () => {
    const largeData = Array.from({ length: 25 }, (_, i) => ({
      id: i,
      name: i % 2 === 0 ? `Apple ${i}` : `Banana ${i}`,
      value: i * 10,
    }));

    render(
      <DataTable
        columns={mockColumns}
        data={largeData}
        pageSize={10}
        getRowKey={(item) => item.id.toString()}
      />,
    );

    const filterInput = screen.getAllByPlaceholderText("Filter name")[0];
    assert(filterInput);
    fireEvent.change(filterInput, { target: { value: "Apple" } });

    expect(screen.getByText(/filtered from 25/)).toBeDefined();
  });

  it("navigates between pages", () => {
    const largeData = Array.from({ length: 25 }, (_, i) => ({
      id: i,
      name: `Item ${i}`,
      value: i * 10,
    }));

    render(
      <DataTable
        columns={mockColumns}
        data={largeData}
        pageSize={10}
        getRowKey={(item) => item.id.toString()}
      />,
    );

    expect(screen.getByText("Item 0")).toBeDefined();
    expect(screen.queryByText("Item 10")).toBeNull();

    // Click next page button
    const nextButton = screen.getByText("»");
    fireEvent.click(nextButton);

    expect(screen.queryByText("Item 0")).toBeNull();
    expect(screen.getByText("Item 10")).toBeDefined();
  });

  it("disables pagination buttons appropriately", () => {
    const largeData = Array.from({ length: 25 }, (_, i) => ({
      id: i,
      name: `Item ${i}`,
      value: i * 10,
    }));

    render(
      <DataTable
        columns={mockColumns}
        data={largeData}
        pageSize={10}
        getRowKey={(item) => item.id.toString()}
      />,
    );

    const firstPageButton = screen.getByText("««");
    const prevButton = screen.getByText("«");

    // On first page, first and prev should be disabled
    expect((firstPageButton as HTMLButtonElement).disabled).toBe(true);
    expect((prevButton as HTMLButtonElement).disabled).toBe(true);

    // Navigate to last page
    const lastPageButton = screen.getByText("»»");
    fireEvent.click(lastPageButton);

    const nextButton = screen.getByText("»");

    // On last page, last and next should be disabled
    expect((nextButton as HTMLButtonElement).disabled).toBe(true);
    expect((lastPageButton as HTMLButtonElement).disabled).toBe(true);
  });

  it("uses custom sort function when provided", () => {
    render(
      <DataTable
        columns={mockColumns}
        data={mockData}
        pageSize={10}
        getRowKey={(item) => item.id.toString()}
      />,
    );

    const valueHeader = screen.getByText("Value").parentElement;
    fireEvent.click(valueHeader!);

    const rows = screen.getAllByRole("row");
    expect(rows[2]?.textContent).toContain("50"); // Banana - lowest value
  });

  it("resets to page 1 when filter changes", () => {
    const largeData = Array.from({ length: 25 }, (_, i) => ({
      id: i,
      name: `Item ${i}`,
      value: i * 10,
    }));

    render(
      <DataTable
        columns={mockColumns}
        data={largeData}
        pageSize={10}
        getRowKey={(item) => item.id.toString()}
      />,
    );

    // Go to page 2
    const nextButton = screen.getByText("»");
    fireEvent.click(nextButton);
    expect(screen.getByText(/Page 2 of 3/)).toBeDefined();

    // Apply filter
    const filterInput = screen.getAllByPlaceholderText("Filter name")[0];
    assert(filterInput);
    fireEvent.change(filterInput, { target: { value: "1" } });

    // Should be back on page 1
    expect(screen.getByText(/Page 1 of/)).toBeDefined();
  });
});
