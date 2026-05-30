import { cleanup, fireEvent, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { renderWithRouter } from "../../test/renderWithRouter";
import { RowMutationSidebar } from "./RowMutationSidebar";

describe("RowMutationSidebar", () => {
  afterEach(() => {
    cleanup();
  });

  it("shows a related-table link for reference fields", async () => {
    renderWithRouter(
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
      />,
    );

    const field = await screen.findByLabelText("chat_id field");
    const showLink = within(field).getByRole("link", { name: "Show chat_id in chats" });
    const url = new URL(showLink.getAttribute("href") ?? "", "https://inspector.test");

    expect(showLink.textContent).toBe("Show");
    expect(url.pathname).toBe("/conn/connection/main/schema/data-explorer/chats/data");
    expect(JSON.parse(url.searchParams.get("filters") ?? "[]")).toMatchObject([
      {
        column: "id",
        operator: "eq",
        value: "chat-42",
      },
    ]);
  });

  it("renders boolean fields as select dropdowns and saves changed values", async () => {
    const onSave = vi.fn();

    renderWithRouter(
      <RowMutationSidebar
        mode="edit"
        tableName="todos"
        schemaColumns={[{ name: "done", column_type: { type: "Boolean" }, nullable: false }]}
        targetRowId="todo-1"
        rowValues={{ done: true }}
        onCancel={vi.fn()}
        onSave={onSave}
      />,
    );

    const field = await screen.findByLabelText("done field");
    const select = within(field).getByRole("combobox");
    const sidebar = field.closest("aside");

    expect((select as HTMLSelectElement).value).toBe("true");
    expect(sidebar).not.toBeNull();

    fireEvent.change(select, { target: { value: "false" } });
    fireEvent.click(within(sidebar as HTMLElement).getByRole("button", { name: "Save" }));

    expect(onSave).toHaveBeenCalledWith({ done: false });
  });

  it("does not render a cancel button for edit mode when closing is disabled", () => {
    renderWithRouter(
      <RowMutationSidebar
        mode="edit"
        tableName="todos"
        schemaColumns={[{ name: "title", column_type: { type: "Text" }, nullable: false }]}
        targetRowId="todo-1"
        rowValues={{ title: "hello" }}
        onSave={vi.fn()}
      />,
    );

    expect(screen.queryByRole("button", { name: "Cancel" })).toBeNull();
  });
});
