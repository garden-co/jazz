import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import { RowMutationSidebar } from "./RowMutationSidebar";

describe("RowMutationSidebar", () => {
  afterEach(() => {
    cleanup();
  });

  it("shows a related-table link for reference fields", () => {
    render(
      <MemoryRouter>
        <RowMutationSidebar
          mode="edit"
          tableName="messages"
          schemaColumns={[
            { name: "body", column_type: { type: "Text" }, nullable: false },
            { name: "chat_id", column_type: { type: "Uuid" }, nullable: true, references: "chats" },
          ]}
          targetRowId="message-1"
          rowValues={{
            body: "hello",
            chat_id: "chat-42",
          }}
          onCancel={vi.fn()}
          onSave={vi.fn()}
        />
      </MemoryRouter>,
    );

    const field = screen.getByLabelText("chat_id field");
    const showLink = within(field).getByRole("link", { name: "Show chat_id in chats" });
    const url = new URL(showLink.getAttribute("href") ?? "", "https://inspector.test");

    expect(showLink.textContent).toBe("Show");
    expect(url.pathname).toBe("/data-explorer/chats/data");
    expect(JSON.parse(url.searchParams.get("filters") ?? "[]")).toMatchObject([
      {
        column: "id",
        operator: "eq",
        value: "chat-42",
      },
    ]);
  });

  it("renders boolean fields as checkboxes and saves unchecked values as false", () => {
    const onSave = vi.fn();

    render(
      <MemoryRouter>
        <RowMutationSidebar
          mode="edit"
          tableName="todos"
          schemaColumns={[{ name: "done", column_type: { type: "Boolean" }, nullable: false }]}
          targetRowId="todo-1"
          rowValues={{ done: true }}
          onCancel={vi.fn()}
          onSave={onSave}
        />
      </MemoryRouter>,
    );

    const field = screen.getByLabelText("done field");
    const checkbox = within(field).getByRole("checkbox", { name: "done" });
    const sidebar = field.closest("aside");

    expect((checkbox as HTMLInputElement).checked).toBe(true);
    expect(sidebar).not.toBeNull();

    fireEvent.click(checkbox);
    fireEvent.click(within(sidebar as HTMLElement).getByRole("button", { name: "Save" }));

    expect(onSave).toHaveBeenCalledWith({ done: false });
  });

  it("does not render a cancel button for edit mode when closing is disabled", () => {
    render(
      <MemoryRouter>
        <RowMutationSidebar
          mode="edit"
          tableName="todos"
          schemaColumns={[{ name: "title", column_type: { type: "Text" }, nullable: false }]}
          targetRowId="todo-1"
          rowValues={{ title: "hello" }}
          onSave={vi.fn()}
        />
      </MemoryRouter>,
    );

    expect(screen.queryByRole("button", { name: "Cancel" })).toBeNull();
  });
});
