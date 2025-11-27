// @vitest-environment happy-dom
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { setActiveAccount, setupJazzTestSync } from "jazz-tools/testing";
import { co } from "jazz-tools";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { CoPlainTextView } from "../../viewer/co-plain-text-view";
import { setup } from "goober";
import React from "react";
import { CoID, JsonObject, RawCoPlainText, RawCoValue } from "cojson";
import { createJazzTestAccount } from "jazz-tools/testing";
import { loadCoValueOrFail } from "../../../tools/tests/utils";

describe("CoPlainTextView", async () => {
  const account = await setupJazzTestSync();
  setActiveAccount(account);

  beforeAll(() => {
    setup(React.createElement);
  });

  afterEach(() => {
    cleanup();
  });

  describe("Edit", () => {
    it("should show edit button when user has write permissions", async () => {
      const value = co.plainText().create("Hello", account);
      const data = value.$jazz.raw.toJSON() as unknown as JsonObject;

      render(
        <CoPlainTextView
          data={data}
          coValue={value.$jazz.raw}
          node={account.$jazz.localNode}
        />,
      );

      // The edit button contains an Icon (SVG), so we look for buttons containing SVG
      expect(screen.getByTitle("Edit")).toBeDefined();
    });

    it("should not show edit button when user does not have write permissions", async () => {
      // Create a new account without write permissions
      const readOnlyAccount = await createJazzTestAccount();
      const group = co.group().create({ owner: account });
      group.addMember(readOnlyAccount, "reader");

      const value = co.plainText().create("Hello", group);

      const valueOnReader = await loadCoValueOrFail(
        readOnlyAccount.$jazz.localNode,
        value.$jazz.id as CoID<RawCoValue>,
      );
      const data = valueOnReader.toJSON() as unknown as JsonObject;

      render(
        <CoPlainTextView
          data={data}
          coValue={valueOnReader as RawCoPlainText}
          node={readOnlyAccount.$jazz.localNode}
        />,
      );

      // Should not have edit button (no buttons with SVG icons)
      expect(screen.queryByTitle("Edit")).toBeNull();
    });

    it("should open edit mode when edit button is clicked", async () => {
      const value = co.plainText().create("Hello, world!", account);
      const data = value.$jazz.raw.toJSON() as unknown as JsonObject;

      render(
        <CoPlainTextView
          data={data}
          coValue={value.$jazz.raw}
          node={account.$jazz.localNode}
        />,
      );

      const editButton = screen.getByTitle("Edit");
      expect(editButton).toBeDefined();

      fireEvent.click(editButton);

      await waitFor(() => {
        const textarea = screen.getByRole("textbox");
        expect(textarea).toBeDefined();
        expect((textarea as HTMLTextAreaElement).value).toBe("Hello, world!");
      });

      expect(screen.getByText("Cancel")).toBeDefined();
      expect(screen.getByText("Save")).toBeDefined();
    });

    it("should save changes when save button is clicked", async () => {
      const value = co.plainText().create("Original", account);
      const data = value.$jazz.raw.toJSON() as unknown as JsonObject;

      render(
        <CoPlainTextView
          data={data}
          coValue={value.$jazz.raw}
          node={account.$jazz.localNode}
        />,
      );

      const editButton = screen.getByTitle("Edit");
      fireEvent.click(editButton);

      await waitFor(() => {
        const textarea = screen.getByRole("textbox");
        fireEvent.change(textarea, { target: { value: "Updated text" } });
      });

      const saveButton = screen.getByText("Save");
      fireEvent.click(saveButton);
    });
  });
});
