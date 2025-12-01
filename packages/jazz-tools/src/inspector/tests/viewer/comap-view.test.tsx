// @vitest-environment happy-dom
import { afterEach, assert, beforeAll, describe, expect, it } from "vitest";
import { createJazzTestAccount, setupJazzTestSync } from "jazz-tools/testing";
import { co, z } from "jazz-tools";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { CoMapView } from "../../viewer/co-map-view";
import { setup } from "goober";
import React from "react";
import { JsonObject } from "cojson";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe("CoMapView", async () => {
  const account = await setupJazzTestSync();

  beforeAll(() => {
    setup(React.createElement);
  });

  afterEach(() => {
    cleanup();
  });

  describe("Basic Rendering", () => {
    it("should render GridView with data", async () => {
      const value = co
        .map({
          pet: z.string(),
          age: z.number(),
        })
        .create({ pet: "dog", age: 10 }, account);

      const data = value.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={value.$jazz.raw}
          data={data}
          node={account.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      expect(screen.getByText("pet")).toBeDefined();
      expect(screen.getByText("age")).toBeDefined();
      expect(screen.getByText("dog")).toBeDefined();
      expect(screen.getByText("10")).toBeDefined();
    });

    it("should render restore button", async () => {
      const value = co
        .map({
          foo: z.string(),
        })
        .create({ foo: "bar" }, account);

      const data = value.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={value.$jazz.raw}
          data={data}
          node={account.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      const restoreButton = screen.getByTitle("Timeline");
      expect(restoreButton).toBeDefined();
    });
  });

  describe("Timestamp Selection", () => {
    it("should display timestamps and allow selection", async () => {
      const value = co
        .map({
          pet: z.string(),
        })
        .create({ pet: "dog" }, account);

      await sleep(2);
      value.$jazz.set("pet", "cat");
      await sleep(2);
      value.$jazz.set("pet", "bird");

      const data = value.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={value.$jazz.raw}
          data={data}
          node={account.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      const restoreButton = screen.getByTitle("Timeline");
      fireEvent.click(restoreButton);

      const slider = screen.getByRole("slider") as HTMLInputElement;
      expect(slider).toBeDefined();
      expect(slider.max).toBe("2");

      fireEvent.change(slider, { target: { value: "0" } });
      expect(slider.value).toBe("0");

      fireEvent.change(slider, { target: { value: "1" } });
      expect(slider.value).toBe("1");
    });

    it("should show timestamp in ISO format", async () => {
      const value = co
        .map({
          foo: z.string(),
        })
        .create({ foo: "bar" }, account);

      value.$jazz.set("foo", "baz");

      const data = value.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={value.$jazz.raw}
          data={data}
          node={account.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      const restoreButton = screen.getByTitle("Timeline");
      fireEvent.click(restoreButton);

      const timestampDisplay = screen.getAllByText(/\d{4}-\d{2}-\d{2}T/)[0];
      expect(timestampDisplay).toBeDefined();
    });

    it("should update preview when timestamp changes", async () => {
      const value = co
        .map({
          pet: z.string(),
        })
        .create({ pet: "dog" }, account);

      // wait to have different timestamps in transactions
      await sleep(2);
      value.$jazz.set("pet", "cat");
      await sleep(2);
      value.$jazz.set("pet", "bird");

      const data = value.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={value.$jazz.raw}
          data={data}
          node={account.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      const restoreButton = screen.getByTitle("Timeline");
      fireEvent.click(restoreButton);

      const slider = screen.getByRole("slider") as HTMLInputElement;
      const preview = screen.getByText(/State at that time:/);
      expect(preview).toBeDefined();

      // Modal starts at the most recent timestamp (last index)
      await waitFor(() => {
        const previewPre = preview.parentElement?.querySelector("pre");
        expect(previewPre?.textContent).toContain("bird");
      });

      fireEvent.change(slider, { target: { value: 0 } });
      await waitFor(() => {
        const previewPre = preview.parentElement?.querySelector("pre");
        expect(previewPre?.textContent).toContain("dog");
      });
    });
  });

  describe("Remove Unknown Properties Checkbox", () => {
    it("should toggle checkbox state", async () => {
      const value = co
        .map({
          foo: z.string(),
        })
        .create({ foo: "bar" }, account);

      value.$jazz.set("foo", "baz");

      const data = value.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={value.$jazz.raw}
          data={data}
          node={account.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      const restoreButton = screen.getByTitle("Timeline");
      fireEvent.click(restoreButton);

      const checkbox = screen.getByRole("checkbox") as HTMLInputElement;
      expect(checkbox.checked).toBe(false);

      fireEvent.click(checkbox);
      expect(checkbox.checked).toBe(true);

      fireEvent.click(checkbox);
      expect(checkbox.checked).toBe(false);
    });
  });

  describe("Restore Functionality", () => {
    it("should show preview of state to restore", async () => {
      const value = co
        .map({
          pet: z.string(),
          age: z.number(),
        })
        .create({ pet: "dog", age: 10 }, account);

      await sleep(2);
      value.$jazz.set("pet", "cat");
      value.$jazz.set("age", 20);

      const data = value.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={value.$jazz.raw}
          data={data}
          node={account.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      const restoreButton = screen.getByTitle("Timeline");
      fireEvent.click(restoreButton);

      const slider = screen.getByRole("slider") as HTMLInputElement;
      fireEvent.change(slider, { target: { value: 0 } });

      await waitFor(() => {
        const preview = screen.getByText(/State at that time:/);
        const previewPre = preview.parentElement?.querySelector("pre");
        expect(previewPre?.textContent).toContain("dog");
        expect(previewPre?.textContent).toContain("10");
      });
    });

    it("should close modal when restore is clicked", async () => {
      const value = co
        .map({
          pet: z.string(),
          age: z.number(),
        })
        .create({ pet: "dog", age: 10 }, account);

      await sleep(2);
      value.$jazz.set("pet", "cat");

      const data = value.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={value.$jazz.raw}
          data={data}
          node={account.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      const restoreButton = screen.getByTitle("Timeline");
      fireEvent.click(restoreButton);

      expect(screen.getByText("Select Timestamp")).toBeDefined();

      const slider = screen.getByRole("slider") as HTMLInputElement;
      fireEvent.change(slider, { target: { value: 0 } });

      const restoreActionButton = screen.getByText("Restore");
      fireEvent.click(restoreActionButton);

      await waitFor(() => {
        expect(screen.queryByText("Select Timestamp")).toBeNull();
      });
    });

    it("should allow selecting timestamp and checking remove properties option", async () => {
      const value = co
        .map({
          pet: z.string(),
          age: z.number().optional(),
        })
        .create({ pet: "dog" }, account);

      await sleep(2);
      value.$jazz.set("age", 10);
      await sleep(2);
      value.$jazz.set("pet", "cat");

      const data = value.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={value.$jazz.raw}
          data={data}
          node={account.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      const restoreButton = screen.getByTitle("Timeline");
      fireEvent.click(restoreButton);

      const checkbox = screen.getByRole("checkbox") as HTMLInputElement;
      expect(checkbox.checked).toBe(false);

      fireEvent.click(checkbox);
      expect(checkbox.checked).toBe(true);

      // Change to earlier timestamp
      const slider = screen.getByRole("slider") as HTMLInputElement;
      fireEvent.change(slider, { target: { value: "0" } });

      await waitFor(() => {
        const preview = screen.getByText(/State at that time:/);
        const previewPre = preview.parentElement?.querySelector("pre");
        expect(previewPre?.textContent).toContain("dog");
      });

      const restoreActionButton = screen.getByText(
        "Restore",
      ) as HTMLButtonElement;
      expect(restoreActionButton.disabled).toBe(false);
    });
  });

  describe("Edge Cases", () => {
    it("should handle empty CoMap", async () => {
      const value = co.map({}).create({}, account);

      const data = value.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={value.$jazz.raw}
          data={data}
          node={account.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      expect(screen.getByTitle("Timeline")).toBeDefined();
    });

    it("should handle complex data types", async () => {
      const value = co
        .map({
          obj: z.object({
            name: z.string(),
            count: z.number(),
          }),
          date: z.date(),
          bool: z.boolean(),
        })
        .create(
          {
            obj: { name: "test", count: 42 },
            date: new Date("2024-01-01"),
            bool: true,
          },
          account,
        );

      const data = value.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={value.$jazz.raw}
          data={data}
          node={account.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      expect(screen.getByText("obj")).toBeDefined();
      expect(screen.getByText("date")).toBeDefined();
      expect(screen.getByText("bool")).toBeDefined();
    });

    it("should not allow restoring to same state (no changes)", async () => {
      const value = co
        .map({
          pet: z.string(),
        })
        .create({ pet: "dog" }, account);

      const data = value.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={value.$jazz.raw}
          data={data}
          node={account.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      const restoreButton = screen.getByTitle("Timeline");
      fireEvent.click(restoreButton);

      expect(screen.queryByRole("slider")).toBeNull();
    });

    it("should handle multiple property changes at different times", async () => {
      const value = co
        .map({
          a: z.string(),
          b: z.string(),
          c: z.string(),
        })
        .create({ a: "1", b: "2", c: "3" }, account);

      await sleep(2);
      value.$jazz.set("a", "4");
      await sleep(2);
      value.$jazz.set("b", "5");
      await sleep(2);
      value.$jazz.set("c", "6");

      const data = value.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={value.$jazz.raw}
          data={data}
          node={account.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      const restoreButton = screen.getByTitle("Timeline");
      fireEvent.click(restoreButton);

      const slider = screen.getByRole("slider") as HTMLInputElement;

      // Verify we can navigate to initial state
      fireEvent.change(slider, { target: { value: 0 } });
      await waitFor(() => {
        const preview = screen.getByText(/State at that time:/);
        const previewPre = preview.parentElement?.querySelector("pre");
        expect(previewPre?.textContent).toContain("1");
        expect(previewPre?.textContent).toContain("2");
        expect(previewPre?.textContent).toContain("3");
      });

      // Verify we can navigate to latest state
      fireEvent.change(slider, { target: { value: slider.max } });
      await waitFor(() => {
        const preview = screen.getByText(/State at that time:/);
        const previewPre = preview.parentElement?.querySelector("pre");
        expect(previewPre?.textContent).toContain("4");
        expect(previewPre?.textContent).toContain("5");
        expect(previewPre?.textContent).toContain("6");
      });
    });
  });

  describe("Preview Display", () => {
    it("should show JSON preview of selected state", async () => {
      const value = co
        .map({
          pet: z.string(),
          age: z.number(),
        })
        .create({ pet: "dog", age: 10 }, account);

      // wait to have different timestamps in transactions
      await sleep(2);

      value.$jazz.set("pet", "cat");

      const data = value.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={value.$jazz.raw}
          data={data}
          node={account.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      const restoreButton = screen.getByTitle("Timeline");
      fireEvent.click(restoreButton);

      // Modal starts at most recent timestamp
      const preview = screen.getByText(/State at that time:/);
      await waitFor(() => {
        const previewPre = preview.parentElement?.querySelector("pre");
        expect(previewPre?.textContent).toContain("cat");
      });

      const slider = screen.getByRole("slider") as HTMLInputElement;
      fireEvent.change(slider, { target: { value: "0" } });

      await waitFor(
        () => {
          const previewPre = preview.parentElement?.querySelector("pre");
          expect(previewPre?.textContent).toContain("dog");
        },
        {
          timeout: 1000,
        },
      );
    });

    it("should update preview when slider moves", async () => {
      const value = co
        .map({
          counter: z.number(),
        })
        .create({ counter: 1 }, account);

      // wait to have different timestamps in transactions
      await sleep(2);
      value.$jazz.set("counter", 2);
      await sleep(2);
      value.$jazz.set("counter", 3);
      await sleep(2);
      value.$jazz.set("counter", 4);

      const data = value.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={value.$jazz.raw}
          data={data}
          node={account.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      const restoreButton = screen.getByTitle("Timeline");
      fireEvent.click(restoreButton);

      const slider = screen.getByRole("slider") as HTMLInputElement;
      const preview = screen.getByText(/State at that time:/);

      // Modal starts at most recent timestamp (counter: 4)
      await waitFor(() => {
        const previewPre = preview.parentElement?.querySelector("pre");
        expect(previewPre?.textContent).toContain('"counter": 4');
      });

      fireEvent.change(slider, { target: { value: "0" } });
      await waitFor(() => {
        const previewPre = preview.parentElement?.querySelector("pre");
        expect(previewPre?.textContent).toContain('"counter": 1');
      });

      fireEvent.change(slider, { target: { value: "2" } });
      await waitFor(() => {
        const previewPre = preview.parentElement?.querySelector("pre");
        expect(previewPre?.textContent).toContain('"counter": 3');
      });
    });
  });

  describe("Permissions", () => {
    it("should disable Add Property button for reader account", async () => {
      const reader = await createJazzTestAccount();
      const group = co.group().create({ owner: account });
      group.addMember(reader, "reader");

      const schema = co.map({
        pet: z.string(),
      });

      const value = schema.create({ pet: "dog" }, group);

      const valueOnReader = await schema.load(value.$jazz.id, {
        loadAs: reader,
      });
      assert(valueOnReader.$isLoaded);
      const data = valueOnReader.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={valueOnReader.$jazz.raw}
          data={data}
          node={reader.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      const addButton = screen.getByTitle("Add Property");
      expect(addButton).toBeDefined();
      expect((addButton as HTMLButtonElement).disabled).toBe(true);
    });

    it("should enable Add Property button for writer account", async () => {
      const writer = await createJazzTestAccount();
      const group = co.group().create({ owner: account });
      group.addMember(writer, "writer");

      const schema = co.map({
        pet: z.string(),
      });

      const value = schema.create({ pet: "dog" }, group);

      const valueOnWriter = await schema.load(value.$jazz.id, {
        loadAs: writer,
      });
      assert(valueOnWriter.$isLoaded);
      const data = valueOnWriter.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={valueOnWriter.$jazz.raw}
          data={data}
          node={writer.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      const addButton = screen.getByTitle("Add Property");
      expect(addButton).toBeDefined();
      expect((addButton as HTMLButtonElement).disabled).toBe(false);
    });

    it("should hide restore buttons for reader account when multiple timestamps exist", async () => {
      const reader = await createJazzTestAccount();
      const group = co.group().create({ owner: account });
      group.addMember(reader, "reader");

      const schema = co.map({
        pet: z.string(),
      });

      const value = schema.create({ pet: "dog" }, group);
      await sleep(2);
      value.$jazz.set("pet", "cat");

      const valueOnReader = await schema.load(value.$jazz.id, {
        loadAs: reader,
      });
      assert(valueOnReader.$isLoaded);
      const data = valueOnReader.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={valueOnReader.$jazz.raw}
          data={data}
          node={reader.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      const restoreButton = screen.getByTitle("Timeline");
      fireEvent.click(restoreButton);

      await waitFor(() => {
        expect(screen.getByText("Select Timestamp")).toBeDefined();
      });

      expect(screen.queryByText("Restore")).toBeNull();
      expect(screen.queryByRole("checkbox")).toBeNull();
    });

    it("should show restore buttons for writer account when multiple timestamps exist", async () => {
      const writer = await createJazzTestAccount();
      const group = co.group().create({ owner: account });
      group.addMember(writer, "writer");

      const schema = co.map({
        pet: z.string(),
      });

      const value = schema.create({ pet: "dog" }, group);
      await sleep(2);
      value.$jazz.set("pet", "cat");

      const valueOnWriter = await schema.load(value.$jazz.id, {
        loadAs: writer,
      });
      assert(valueOnWriter.$isLoaded);
      const data = valueOnWriter.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={valueOnWriter.$jazz.raw}
          data={data}
          node={writer.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      const restoreButton = screen.getByTitle("Timeline");
      fireEvent.click(restoreButton);

      await waitFor(() => {
        expect(screen.getByText("Restore")).toBeDefined();
      });

      expect(screen.getByRole("checkbox")).toBeDefined();
    });

    it("should hide edit buttons in GridView for reader account", async () => {
      const reader = await createJazzTestAccount();
      const group = co.group().create({ owner: account });
      group.addMember(reader, "reader");

      const schema = co.map({
        pet: z.string(),
        age: z.number(),
      });

      const value = schema.create({ pet: "dog", age: 10 }, group);

      const valueOnReader = await schema.load(value.$jazz.id, {
        loadAs: reader,
      });
      assert(valueOnReader.$isLoaded);
      const data = valueOnReader.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={valueOnReader.$jazz.raw}
          data={data}
          node={reader.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      expect(screen.getByText("pet")).toBeDefined();
      expect(screen.getByText("age")).toBeDefined();

      const editButtons = screen.queryAllByLabelText("Edit");
      const deleteButtons = screen.queryAllByLabelText("Delete");

      expect(editButtons).toHaveLength(0);
      expect(deleteButtons).toHaveLength(0);
    });

    it("should show edit buttons in GridView for writer account", async () => {
      const writer = await createJazzTestAccount();
      const group = co.group().create({ owner: account });
      group.addMember(writer, "writer");

      const schema = co.map({
        pet: z.string(),
        age: z.number(),
      });

      const value = schema.create({ pet: "dog", age: 10 }, group);

      const valueOnWriter = await schema.load(value.$jazz.id, {
        loadAs: writer,
      });
      assert(valueOnWriter.$isLoaded);
      const data = valueOnWriter.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={valueOnWriter.$jazz.raw}
          data={data}
          node={writer.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      expect(screen.getByText("pet")).toBeDefined();
      expect(screen.getByText("age")).toBeDefined();

      const editButtons = screen.queryAllByLabelText("Edit");
      const deleteButtons = screen.queryAllByLabelText("Delete");

      expect(editButtons.length).toBeGreaterThan(0);
      expect(deleteButtons.length).toBeGreaterThan(0);
    });

    it("should enable Add Property button for admin account", async () => {
      const admin = await createJazzTestAccount();
      const group = co.group().create({ owner: account });
      group.addMember(admin, "admin");

      const schema = co.map({
        pet: z.string(),
      });

      const value = schema.create({ pet: "dog" }, group);

      const valueOnAdmin = await schema.load(value.$jazz.id, {
        loadAs: admin,
      });
      assert(valueOnAdmin.$isLoaded);
      const data = valueOnAdmin.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={valueOnAdmin.$jazz.raw}
          data={data}
          node={admin.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      const addButton = screen.getByTitle("Add Property");
      expect(addButton).toBeDefined();
      expect((addButton as HTMLButtonElement).disabled).toBe(false);
    });

    it("should enable Add Property button for manager account", async () => {
      const manager = await createJazzTestAccount();
      const group = co.group().create({ owner: account });
      group.addMember(manager, "manager");

      const schema = co.map({
        pet: z.string(),
      });

      const value = schema.create({ pet: "dog" }, group);

      const valueOnManager = await schema.load(value.$jazz.id, {
        loadAs: manager,
      });
      assert(valueOnManager.$isLoaded);
      const data = valueOnManager.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={valueOnManager.$jazz.raw}
          data={data}
          node={manager.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      const addButton = screen.getByTitle("Add Property");
      expect(addButton).toBeDefined();
      expect((addButton as HTMLButtonElement).disabled).toBe(false);
    });

    it("should enable Add Property button for writeOnly account", async () => {
      const writeOnly = await createJazzTestAccount();
      const group = co.group().create({ owner: account });
      group.addMember(writeOnly, "writeOnly");

      const schema = co.map({
        pet: z.string(),
      });

      const value = schema.create({ pet: "dog" }, group);

      const valueOnWriteOnly = await schema.load(value.$jazz.id, {
        loadAs: writeOnly,
      });
      assert(valueOnWriteOnly.$isLoaded);
      const data = valueOnWriteOnly.$jazz.raw.toJSON() as JsonObject;

      render(
        <CoMapView
          coValue={valueOnWriteOnly.$jazz.raw}
          data={data}
          node={writeOnly.$jazz.localNode}
          onNavigate={() => {}}
        />,
      );

      const addButton = screen.getByTitle("Add Property");
      expect(addButton).toBeDefined();
      expect((addButton as HTMLButtonElement).disabled).toBe(false);
    });
  });
});
