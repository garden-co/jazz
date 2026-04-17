import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes } from "react-router";
import { TableSchemaDefinition } from "./TableSchemaDefinition";

const mockUseDevtoolsContext = vi.fn();

vi.mock("../../contexts/devtools-context.js", () => ({
  useDevtoolsContext: () => mockUseDevtoolsContext(),
}));

describe("TableSchemaDefinition", () => {
  beforeEach(() => {
    mockUseDevtoolsContext.mockReset();
  });

  afterEach(() => {
    cleanup();
  });

  it("renders schema and standalone permissions for the active table", () => {
    mockUseDevtoolsContext.mockReturnValue({
      runtime: "standalone",
      wasmSchema: {
        users: {
          columns: [{ name: "id", column_type: { type: "Uuid" }, nullable: false }],
        },
      },
      storedPermissions: {
        head: {
          schemaHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          version: 3,
          parentBundleObjectId: "11111111-1111-1111-1111-111111111111",
          bundleObjectId: "22222222-2222-2222-2222-222222222222",
        },
        permissions: {
          users: {
            select: {
              using: {
                type: "True",
              },
            },
          },
        },
      },
    });

    render(
      <MemoryRouter initialEntries={["/data-explorer/users/schema"]}>
        <Routes>
          <Route path="/data-explorer/:table/schema" element={<TableSchemaDefinition />} />
        </Routes>
      </MemoryRouter>,
    );

    expect(screen.getByRole("heading", { name: "users schema" })).not.toBeNull();
    expect(screen.getByRole("heading", { name: "users permissions" })).not.toBeNull();
    expect(screen.getByText(/"columns"/)).not.toBeNull();
    expect(screen.getByText(/"select"/)).not.toBeNull();
  });

  it("shows an empty state when no permissions head has been published", () => {
    mockUseDevtoolsContext.mockReturnValue({
      runtime: "standalone",
      wasmSchema: {
        users: {
          columns: [{ name: "id", column_type: { type: "Uuid" }, nullable: false }],
        },
      },
      storedPermissions: {
        head: null,
        permissions: null,
      },
    });

    render(
      <MemoryRouter initialEntries={["/data-explorer/users/schema"]}>
        <Routes>
          <Route path="/data-explorer/:table/schema" element={<TableSchemaDefinition />} />
        </Routes>
      </MemoryRouter>,
    );

    expect(
      screen.getByText("No published sync-server permissions found for this app."),
    ).not.toBeNull();
  });

  it("shows a table-specific empty state when the current table has no stored permissions", () => {
    mockUseDevtoolsContext.mockReturnValue({
      runtime: "standalone",
      wasmSchema: {
        users: {
          columns: [{ name: "id", column_type: { type: "Uuid" }, nullable: false }],
        },
      },
      storedPermissions: {
        head: {
          schemaHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          version: 3,
          parentBundleObjectId: null,
          bundleObjectId: "22222222-2222-2222-2222-222222222222",
        },
        permissions: {
          projects: {
            select: {
              using: {
                type: "True",
              },
            },
          },
        },
      },
    });

    render(
      <MemoryRouter initialEntries={["/data-explorer/users/schema"]}>
        <Routes>
          <Route path="/data-explorer/:table/schema" element={<TableSchemaDefinition />} />
        </Routes>
      </MemoryRouter>,
    );

    expect(screen.getByText('No stored permissions for table "users".')).not.toBeNull();
  });

  it("hides the permissions section in extension mode", () => {
    mockUseDevtoolsContext.mockReturnValue({
      runtime: "extension",
      wasmSchema: {
        users: {
          columns: [{ name: "id", column_type: { type: "Uuid" }, nullable: false }],
        },
      },
      storedPermissions: {
        head: {
          schemaHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          version: 3,
          parentBundleObjectId: null,
          bundleObjectId: "22222222-2222-2222-2222-222222222222",
        },
        permissions: {
          users: {
            select: {
              using: {
                type: "True",
              },
            },
          },
        },
      },
    });

    render(
      <MemoryRouter initialEntries={["/data-explorer/users/schema"]}>
        <Routes>
          <Route path="/data-explorer/:table/schema" element={<TableSchemaDefinition />} />
        </Routes>
      </MemoryRouter>,
    );

    expect(screen.queryByRole("heading", { name: "users permissions" })).toBeNull();
  });
});
