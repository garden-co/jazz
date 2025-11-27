// @vitest-environment happy-dom
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { setActiveAccount, setupJazzTestSync } from "jazz-tools/testing";
import { co, z } from "jazz-tools";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { CoValueEditor } from "../../viewer/co-value-editor";
import { setup } from "goober";
import React from "react";

describe("CoValueEditor", async () => {
  const account = await setupJazzTestSync();
  setActiveAccount(account);

  beforeAll(() => {
    setup(React.createElement);
  });

  afterEach(() => {
    cleanup();
  });

  describe("Initial Rendering", () => {
    it("should render with number value", async () => {
      const value = co
        .map({
          count: z.number(),
        })
        .create({ count: 42 });

      const onCancel = vi.fn();

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="count"
          value={42}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      expect(screen.getByLabelText("Type")).toBeDefined();
      expect(screen.getByDisplayValue("number")).toBeDefined();
      expect(screen.getByDisplayValue("42")).toBeDefined();
      expect(screen.getByText("Cancel")).toBeDefined();
      expect(screen.getByText("Submit")).toBeDefined();
    });

    it("should render with string value", async () => {
      const value = co
        .map({
          name: z.string(),
        })
        .create({ name: "test" });

      const onCancel = vi.fn();

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="name"
          value="test"
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      expect(screen.getByDisplayValue("string")).toBeDefined();
      expect(screen.getByDisplayValue("test")).toBeDefined();
    });

    it("should render with boolean true value", async () => {
      const value = co
        .map({
          active: z.boolean(),
        })
        .create({ active: true });

      const onCancel = vi.fn();

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="active"
          value={true}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      expect(screen.getByDisplayValue("true")).toBeDefined();
      expect(screen.queryByRole("textbox")).toBeNull();
    });

    it("should render with boolean false value", async () => {
      const value = co
        .map({
          active: z.boolean(),
        })
        .create({ active: false });

      const onCancel = vi.fn();

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="active"
          value={false}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      expect(screen.getByDisplayValue("false")).toBeDefined();
      expect(screen.queryByRole("textbox")).toBeNull();
    });

    it("should render with null value", async () => {
      const value = co
        .map({
          data: z.string().nullable(),
        })
        .create({ data: null });

      const onCancel = vi.fn();

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="data"
          value={null}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      expect(screen.getByDisplayValue("null")).toBeDefined();
      expect(screen.queryByRole("textbox")).toBeNull();
    });

    it("should render with undefined value", async () => {
      const value = co
        .map({
          optional: z.string().optional(),
        })
        .create({});

      const onCancel = vi.fn();

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="optional"
          value={undefined}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      expect(screen.getByDisplayValue("undefined")).toBeDefined();
      expect(screen.queryByRole("textbox")).toBeNull();
    });

    it("should render with object value", async () => {
      const value = co
        .map({
          config: z.json(),
        })
        .create({ config: { foo: "bar", num: 123 } });

      const onCancel = vi.fn();

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="config"
          value={{ foo: "bar", num: 123 }}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      expect(screen.getByDisplayValue("object")).toBeDefined();
      const textarea = screen.getByRole("textbox") as HTMLTextAreaElement;
      expect(textarea).toBeDefined();
      expect(textarea.value).toBe(
        JSON.stringify({ foo: "bar", num: 123 }, null, 2),
      );
    });
  });

  describe("Type Selection", () => {
    it("should show textarea when number type is selected", async () => {
      const value = co
        .map({
          count: z.number(),
        })
        .create({ count: 42 });

      const onCancel = vi.fn();

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="count"
          value={42}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const select = screen.getByLabelText("Type");
      expect(screen.getByRole("textbox")).toBeDefined();

      fireEvent.change(select, { target: { value: "number" } });
      expect(screen.getByRole("textbox")).toBeDefined();
    });

    it("should show textarea when string type is selected", async () => {
      const value = co
        .map({
          name: z.string(),
        })
        .create({ name: "test" });

      const onCancel = vi.fn();

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="name"
          value="test"
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const select = screen.getByLabelText("Type");
      expect(screen.getByRole("textbox")).toBeDefined();

      fireEvent.change(select, { target: { value: "string" } });
      expect(screen.getByRole("textbox")).toBeDefined();
    });

    it("should show textarea when object type is selected", async () => {
      const value = co
        .map({
          data: z.json(),
        })
        .create({ data: {} });

      const onCancel = vi.fn();

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="data"
          value={{}}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const select = screen.getByLabelText("Type");
      expect(screen.getByRole("textbox")).toBeDefined();

      fireEvent.change(select, { target: { value: "object" } });
      expect(screen.getByRole("textbox")).toBeDefined();
    });

    it("should hide textarea when boolean type is selected", async () => {
      const value = co
        .map({
          count: z.number(),
        })
        .create({ count: 42 });

      const onCancel = vi.fn();

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="count"
          value={42}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const select = screen.getByLabelText("Type");
      fireEvent.change(select, { target: { value: "true" } });

      await waitFor(() => {
        expect(screen.queryByRole("textbox")).toBeNull();
      });
    });

    it("should hide textarea when null type is selected", async () => {
      const value = co
        .map({
          count: z.number(),
        })
        .create({ count: 42 });

      const onCancel = vi.fn();

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="count"
          value={42}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const select = screen.getByLabelText("Type");
      fireEvent.change(select, { target: { value: "null" } });

      await waitFor(() => {
        expect(screen.queryByRole("textbox")).toBeNull();
      });
    });

    it("should hide textarea when undefined type is selected", async () => {
      const value = co
        .map({
          count: z.number(),
        })
        .create({ count: 42 });

      const onCancel = vi.fn();

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="count"
          value={42}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const select = screen.getByLabelText("Type");
      fireEvent.change(select, { target: { value: "undefined" } });

      await waitFor(() => {
        expect(screen.queryByRole("textbox")).toBeNull();
      });
    });
  });

  describe("Form Submission", () => {
    it("should submit number value", async () => {
      const value = co
        .map({
          count: z.number(),
        })
        .create({ count: 42 });

      const onCancel = vi.fn();
      const makeTransactionSpy = vi.spyOn(
        value.$jazz.raw.core,
        "makeTransaction",
      );

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="count"
          value={42}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const textarea = screen.getByRole("textbox");
      fireEvent.change(textarea, { target: { value: "100" } });

      const submitButton = screen.getByText("Submit");
      fireEvent.click(submitButton);

      await waitFor(() => {
        expect(makeTransactionSpy).toHaveBeenCalledWith(
          [{ op: "set", key: "count", value: 100 }],
          "private",
        );
        expect(onCancel).toHaveBeenCalled();
      });
    });

    it("should submit string value", async () => {
      const value = co
        .map({
          name: z.string(),
        })
        .create({ name: "test" });

      const onCancel = vi.fn();
      const makeTransactionSpy = vi.spyOn(
        value.$jazz.raw.core,
        "makeTransaction",
      );

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="name"
          value="test"
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const textarea = screen.getByRole("textbox");
      fireEvent.change(textarea, { target: { value: "updated" } });

      const submitButton = screen.getByText("Submit");
      fireEvent.click(submitButton);

      await waitFor(() => {
        expect(makeTransactionSpy).toHaveBeenCalledWith(
          [{ op: "set", key: "name", value: "updated" }],
          "private",
        );
        expect(onCancel).toHaveBeenCalled();
      });
    });

    it("should submit boolean true value", async () => {
      const value = co
        .map({
          active: z.boolean(),
        })
        .create({ active: false });

      const onCancel = vi.fn();
      const makeTransactionSpy = vi.spyOn(
        value.$jazz.raw.core,
        "makeTransaction",
      );

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="active"
          value={false}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const select = screen.getByLabelText("Type");
      fireEvent.change(select, { target: { value: "true" } });

      const submitButton = screen.getByText("Submit");
      fireEvent.click(submitButton);

      await waitFor(() => {
        expect(makeTransactionSpy).toHaveBeenCalledWith(
          [{ op: "set", key: "active", value: true }],
          "private",
        );
        expect(onCancel).toHaveBeenCalled();
      });
    });

    it("should submit boolean false value", async () => {
      const value = co
        .map({
          active: z.boolean(),
        })
        .create({ active: true });

      const onCancel = vi.fn();
      const makeTransactionSpy = vi.spyOn(
        value.$jazz.raw.core,
        "makeTransaction",
      );

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="active"
          value={true}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const select = screen.getByLabelText("Type");
      fireEvent.change(select, { target: { value: "false" } });

      const submitButton = screen.getByText("Submit");
      fireEvent.click(submitButton);

      await waitFor(() => {
        expect(makeTransactionSpy).toHaveBeenCalledWith(
          [{ op: "set", key: "active", value: false }],
          "private",
        );
        expect(onCancel).toHaveBeenCalled();
      });
    });

    it("should submit null value", async () => {
      const value = co
        .map({
          data: z.string().nullable(),
        })
        .create({ data: "test" });

      const onCancel = vi.fn();
      const makeTransactionSpy = vi.spyOn(
        value.$jazz.raw.core,
        "makeTransaction",
      );

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="data"
          value="test"
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const select = screen.getByLabelText("Type");
      fireEvent.change(select, { target: { value: "null" } });

      const submitButton = screen.getByText("Submit");
      fireEvent.click(submitButton);

      await waitFor(() => {
        expect(makeTransactionSpy).toHaveBeenCalledWith(
          [{ op: "set", key: "data", value: null }],
          "private",
        );
        expect(onCancel).toHaveBeenCalled();
      });
    });

    it("should submit undefined value", async () => {
      const value = co
        .map({
          optional: z.string().optional(),
        })
        .create({ optional: "test" });

      const onCancel = vi.fn();
      const makeTransactionSpy = vi.spyOn(
        value.$jazz.raw.core,
        "makeTransaction",
      );

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="optional"
          value="test"
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const select = screen.getByLabelText("Type");
      fireEvent.change(select, { target: { value: "undefined" } });

      const submitButton = screen.getByText("Submit");
      fireEvent.click(submitButton);

      await waitFor(() => {
        expect(makeTransactionSpy).toHaveBeenCalledWith(
          [{ op: "set", key: "optional", value: undefined }],
          "private",
        );
        expect(onCancel).toHaveBeenCalled();
      });
    });

    it("should submit object value", async () => {
      const value = co
        .map({
          config: z.json(),
        })
        .create({ config: {} });

      const onCancel = vi.fn();
      const makeTransactionSpy = vi.spyOn(
        value.$jazz.raw.core,
        "makeTransaction",
      );

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="config"
          value={{}}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const textarea = screen.getByRole("textbox");
      const newObject = { foo: "bar", nested: { value: 123 } };
      fireEvent.change(textarea, {
        target: { value: JSON.stringify(newObject, null, 2) },
      });

      const submitButton = screen.getByText("Submit");
      fireEvent.click(submitButton);

      await waitFor(() => {
        expect(makeTransactionSpy).toHaveBeenCalledWith(
          [{ op: "set", key: "config", value: newObject }],
          "private",
        );
        expect(onCancel).toHaveBeenCalled();
      });
    });

    it("should prevent default form submission", async () => {
      const value = co
        .map({
          count: z.number(),
        })
        .create({ count: 42 });

      const onCancel = vi.fn();

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="count"
          value={42}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const form = screen.getByRole("textbox").closest("form");
      expect(form).toBeDefined();

      const submitEvent = new Event("submit", {
        bubbles: true,
        cancelable: true,
      });
      const preventDefaultSpy = vi.spyOn(submitEvent, "preventDefault");
      const stopPropagationSpy = vi.spyOn(submitEvent, "stopPropagation");

      if (form) {
        fireEvent(form, submitEvent);
      }

      expect(preventDefaultSpy).toHaveBeenCalled();
      expect(stopPropagationSpy).toHaveBeenCalled();
    });
  });

  describe("Cancel Button", () => {
    it("should call onCancel when cancel button is clicked", async () => {
      const value = co
        .map({
          count: z.number(),
        })
        .create({ count: 42 });

      const onCancel = vi.fn();

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="count"
          value={42}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const cancelButton = screen.getByText("Cancel");
      fireEvent.click(cancelButton);

      expect(onCancel).toHaveBeenCalledTimes(1);
    });

    it("should not make transaction when cancel is clicked", async () => {
      const value = co
        .map({
          count: z.number(),
        })
        .create({ count: 42 });

      const onCancel = vi.fn();
      const makeTransactionSpy = vi.spyOn(
        value.$jazz.raw.core,
        "makeTransaction",
      );

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="count"
          value={42}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const cancelButton = screen.getByText("Cancel");
      fireEvent.click(cancelButton);

      expect(makeTransactionSpy).not.toHaveBeenCalled();
    });
  });

  describe("Event Propagation", () => {
    it("should stop propagation on select click", async () => {
      const value = co
        .map({
          count: z.number(),
        })
        .create({ count: 42 });

      const onCancel = vi.fn();

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="count"
          value={42}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const select = screen.getByLabelText("Type");
      const clickEvent = new MouseEvent("click", { bubbles: true });
      const stopPropagationSpy = vi.spyOn(clickEvent, "stopPropagation");

      fireEvent(select, clickEvent);

      expect(stopPropagationSpy).toHaveBeenCalled();
    });

    it("should stop propagation on textarea click", async () => {
      const value = co
        .map({
          count: z.number(),
        })
        .create({ count: 42 });

      const onCancel = vi.fn();

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="count"
          value={42}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const textarea = screen.getByRole("textbox");
      const clickEvent = new MouseEvent("click", { bubbles: true });
      const stopPropagationSpy = vi.spyOn(clickEvent, "stopPropagation");

      fireEvent(textarea, clickEvent);

      expect(stopPropagationSpy).toHaveBeenCalled();
    });
  });

  describe("Edge Cases", () => {
    it("should handle decimal number input", async () => {
      const value = co
        .map({
          count: z.number(),
        })
        .create({ count: 42 });

      const onCancel = vi.fn();
      const makeTransactionSpy = vi.spyOn(
        value.$jazz.raw.core,
        "makeTransaction",
      );

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="count"
          value={42}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const textarea = screen.getByRole("textbox");
      fireEvent.change(textarea, { target: { value: "3.14" } });

      const submitButton = screen.getByText("Submit");
      fireEvent.click(submitButton);

      await waitFor(() => {
        expect(makeTransactionSpy).toHaveBeenCalledWith(
          [{ op: "set", key: "count", value: 3.14 }],
          "private",
        );
      });
    });

    it("should handle empty string input", async () => {
      const value = co
        .map({
          name: z.string(),
        })
        .create({ name: "test" });

      const onCancel = vi.fn();
      const makeTransactionSpy = vi.spyOn(
        value.$jazz.raw.core,
        "makeTransaction",
      );

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="name"
          value="test"
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const textarea = screen.getByRole("textbox");
      fireEvent.change(textarea, { target: { value: "" } });

      const submitButton = screen.getByText("Submit");
      fireEvent.click(submitButton);

      await waitFor(() => {
        expect(makeTransactionSpy).toHaveBeenCalledWith(
          [{ op: "set", key: "name", value: "" }],
          "private",
        );
      });
    });

    it("should handle complex nested object", async () => {
      const value = co
        .map({
          config: z.json(),
        })
        .create({ config: {} });

      const onCancel = vi.fn();
      const makeTransactionSpy = vi.spyOn(
        value.$jazz.raw.core,
        "makeTransaction",
      );

      render(
        <CoValueEditor
          node={account.$jazz.localNode}
          property="config"
          value={{}}
          coValue={value.$jazz.raw}
          onCancel={onCancel}
        />,
      );

      const textarea = screen.getByRole("textbox");
      const complexObject = {
        nested: {
          deep: {
            value: [1, 2, 3],
            items: [{ id: 1 }, { id: 2 }],
          },
        },
      };
      fireEvent.change(textarea, {
        target: { value: JSON.stringify(complexObject, null, 2) },
      });

      const submitButton = screen.getByText("Submit");
      fireEvent.click(submitButton);

      await waitFor(() => {
        expect(makeTransactionSpy).toHaveBeenCalledWith(
          [{ op: "set", key: "config", value: complexObject }],
          "private",
        );
      });
    });
  });
});
