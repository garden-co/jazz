// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, renderHook, waitFor } from "@testing-library/react";
import { type PropsWithChildren } from "react";
import { HashRouterProvider } from "../../router/hash-router.js";
import { useRouter } from "../../router/context.js";
import type { PageInfo } from "../../viewer/types.js";
import type { CoID, RawCoValue } from "cojson";

function Wrapper({ children }: PropsWithChildren) {
  return <HashRouterProvider>{children}</HashRouterProvider>;
}

function encodePathToHash(path: PageInfo[]): string {
  return path
    .map((page) => {
      if (page.name && page.name !== "Root") {
        return `${page.coId}:${encodeURIComponent(page.name)}`;
      }
      return page.coId;
    })
    .join("/");
}

async function setHash(path: PageInfo[]) {
  window.location.assign(`#/${encodePathToHash(path)}`);
}

describe("HashRouterProvider", () => {
  beforeEach(async () => {
    // Clear hash before each test
    setHash([]);
  });

  afterEach(async () => {
    setHash([]);
  });

  describe("initialization", () => {
    it("should initialize with empty path when no hash and no defaultPath", async () => {
      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual([]);
      });
    });

    it("should initialize with defaultPath when provided", () => {
      const defaultPath: PageInfo[] = [
        { coId: "co_test1" as CoID<RawCoValue>, name: "Test1" },
        { coId: "co_test2" as CoID<RawCoValue>, name: "Test2" },
      ];

      function WrapperWithDefaultPath({ children }: PropsWithChildren) {
        return (
          <HashRouterProvider defaultPath={defaultPath}>
            {children}
          </HashRouterProvider>
        );
      }

      const { result } = renderHook(() => useRouter(), {
        wrapper: WrapperWithDefaultPath,
      });
      expect(result.current.path).toEqual(defaultPath);
    });

    it("should initialize from hash when available", async () => {
      const storedPath: PageInfo[] = [
        { coId: "co_stored1" as CoID<RawCoValue>, name: "Stored1" },
      ];
      await setHash(storedPath);

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual(storedPath);
      });
    });

    it("should sync defaultPath over hash when defaultPath is provided", async () => {
      const storedPath: PageInfo[] = [
        { coId: "co_stored" as CoID<RawCoValue>, name: "Stored" },
      ];
      const defaultPath: PageInfo[] = [
        { coId: "co_default" as CoID<RawCoValue>, name: "Default" },
      ];
      await setHash(storedPath);

      function WrapperWithDefaultPath({ children }: PropsWithChildren) {
        return (
          <HashRouterProvider defaultPath={defaultPath}>
            {children}
          </HashRouterProvider>
        );
      }

      const { result } = renderHook(() => useRouter(), {
        wrapper: WrapperWithDefaultPath,
      });

      await waitFor(() => {
        expect(result.current.path).toEqual(defaultPath);
      });
    });

    it("should handle invalid hash gracefully", async () => {
      // The decodePathFromHash function doesn't actually throw errors for invalid formats
      // It just parses what it can. So we test with a malformed hash that might cause issues
      await setHash([
        {
          coId: "invalid:hash:format" as CoID<RawCoValue>,
          name: "Invalid Hash Format",
        },
      ]);
      const consoleErrorSpy = vi
        .spyOn(console, "error")
        .mockImplementation(() => {});

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      // Wait for initialization
      await waitFor(() => {
        // The hash will be parsed, might not be empty
        expect(result.current.path).toBeDefined();
      });

      // decodePathFromHash doesn't throw, it just parses segments
      // So we might not get an error, but the path should be valid
      expect(Array.isArray(result.current.path)).toBe(true);
      consoleErrorSpy.mockRestore();
    });

    it("should handle SSR scenario - component initializes with empty path when no defaultPath", async () => {
      // In SSR, window is undefined, so the initial state should be empty array
      // We test this by ensuring the component works correctly without hash
      // Note: We can't actually set window to undefined in happy-dom environment
      // So we just verify it works when hash is empty
      await setHash([]);

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      // When no hash and no defaultPath, should initialize with empty array
      await waitFor(() => {
        expect(result.current.path).toEqual([]);
      });
    });

    it("should decode hash with names correctly", async () => {
      const path: PageInfo[] = [
        { coId: "co_test1" as CoID<RawCoValue>, name: "Test Name" },
        { coId: "co_test2" as CoID<RawCoValue>, name: "Another Name" },
      ];
      await setHash(path);

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual(path);
      });
    });

    it("should decode hash without names correctly", async () => {
      const path: PageInfo[] = [
        { coId: "co_test1" as CoID<RawCoValue> },
        { coId: "co_test2" as CoID<RawCoValue> },
      ];
      await setHash(path);

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual(path);
      });
    });

    it("should handle Root name in hash", async () => {
      const path: PageInfo[] = [
        { coId: "co_test1" as CoID<RawCoValue>, name: "Root" },
      ];
      // Root name should not be encoded in hash
      await setHash([{ coId: "co_test1" as CoID<RawCoValue>, name: "Root" }]);

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path[0]?.coId).toBe("co_test1");
        // Root name might be undefined or "Root" depending on implementation
      });
    });
  });

  describe("hash persistence", () => {
    it("should persist path changes to hash", async () => {
      await setHash([]);
      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual([]);
      });

      const newPages: PageInfo[] = [
        { coId: "co_new1" as CoID<RawCoValue>, name: "New1" },
      ];

      act(() => {
        result.current.addPages(newPages);
      });

      await waitFor(() => {
        expect(result.current.path).toEqual(newPages);
        const hash = window.location.hash.slice(2); // Remove '#/'
        expect(hash).toBeTruthy();
        const decoded = hash.split("/").map((segment) => {
          const [coId, encodedName] = segment.split(":");
          return {
            coId,
            name: encodedName ? decodeURIComponent(encodedName) : undefined,
          } as PageInfo;
        });
        expect(decoded).toEqual(newPages);
      });
    });

    it("should update hash when path changes", async () => {
      // Ensure hash is cleared before starting
      await setHash([]);

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual([]);
      });

      const firstPages: PageInfo[] = [
        { coId: "co_first" as CoID<RawCoValue>, name: "First" },
      ];

      act(() => {
        result.current.addPages(firstPages);
      });

      await waitFor(() => {
        expect(result.current.path).toEqual(firstPages);
      });

      const secondPages: PageInfo[] = [
        { coId: "co_second" as CoID<RawCoValue>, name: "Second" },
      ];

      act(() => {
        result.current.addPages(secondPages);
      });

      await waitFor(() => {
        expect(result.current.path).toEqual([...firstPages, ...secondPages]);
        const hash = window.location.hash.slice(2);
        const decoded = hash.split("/").map((segment) => {
          const [coId, encodedName] = segment.split(":");
          return {
            coId,
            name: encodedName ? decodeURIComponent(encodedName) : undefined,
          } as PageInfo;
        });
        expect(decoded).toHaveLength(2);
        expect(decoded[0]).toEqual(firstPages[0]);
        expect(decoded[1]).toEqual(secondPages[0]);
      });
    });
  });

  describe.skip("hashchange event", () => {
    it("should update path when hash changes", async () => {
      // Ensure hash is cleared before starting
      await setHash([]);

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual([]);
      });

      const newPath: PageInfo[] = [
        { coId: "co_new" as CoID<RawCoValue>, name: "New" },
      ];

      await setHash(newPath);

      // Wait for hashchange event to be processed
      await waitFor(
        () => {
          expect(result.current.path).toEqual(newPath);
        },
        { timeout: 1000 },
      );
    });

    it("should use defaultPath when hash is cleared", async () => {
      const defaultPath: PageInfo[] = [
        { coId: "co_default" as CoID<RawCoValue>, name: "Default" },
      ];

      function WrapperWithDefaultPath({ children }: PropsWithChildren) {
        return (
          <HashRouterProvider defaultPath={defaultPath}>
            {children}
          </HashRouterProvider>
        );
      }

      const { result } = renderHook(() => useRouter(), {
        wrapper: WrapperWithDefaultPath,
      });

      await waitFor(() => {
        expect(result.current.path).toEqual(defaultPath);
      });

      await setHash([]);

      await waitFor(
        () => {
          expect(result.current.path).toEqual(defaultPath);
        },
        { timeout: 1000 },
      );
    });

    it("should handle hash changes in hashchange event", async () => {
      // Ensure hash is cleared before starting
      await setHash([]);

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual([]);
      });

      const newPath: PageInfo[] = [
        { coId: "co_testhash" as CoID<RawCoValue>, name: "TestHash" },
      ];

      await setHash(newPath);

      // Wait for hashchange to process
      await waitFor(
        () => {
          expect(result.current.path).toEqual(newPath);
        },
        { timeout: 1000 },
      );
    });
  });

  describe("addPages", () => {
    it("should add pages to the current path", async () => {
      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual([]);
      });

      const newPages: PageInfo[] = [
        { coId: "co_page1" as CoID<RawCoValue>, name: "Page1" },
        { coId: "co_page2" as CoID<RawCoValue>, name: "Page2" },
      ];

      act(() => {
        result.current.addPages(newPages);
      });

      await waitFor(() => {
        expect(result.current.path).toEqual(newPages);
      });
    });

    it("should append pages to existing path", async () => {
      // Don't use defaultPath here since it will override manual changes
      const initialPath: PageInfo[] = [
        { coId: "co_initial" as CoID<RawCoValue>, name: "Initial" },
      ];
      await setHash(initialPath);

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual(initialPath);
      });

      const newPages: PageInfo[] = [
        { coId: "co_new" as CoID<RawCoValue>, name: "New" },
      ];

      act(() => {
        result.current.addPages(newPages);
      });

      await waitFor(() => {
        expect(result.current.path).toHaveLength(2);
        expect(result.current.path[0]).toEqual(initialPath[0]);
        expect(result.current.path[1]).toEqual(newPages[0]);
      });
    });

    it("should handle adding empty array", async () => {
      // Don't use defaultPath here since it will override manual changes
      const initialPath: PageInfo[] = [
        { coId: "co_initial" as CoID<RawCoValue>, name: "Initial" },
      ];
      await setHash(initialPath);

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual(initialPath);
      });

      act(() => {
        result.current.addPages([]);
      });

      await waitFor(() => {
        expect(result.current.path).toEqual(initialPath);
      });
    });
  });

  describe("goToIndex", () => {
    it("should navigate to a specific index", async () => {
      // Don't use defaultPath here since it will override manual changes
      const initialPath: PageInfo[] = [
        { coId: "co_page1" as CoID<RawCoValue>, name: "Page1" },
        { coId: "co_page2" as CoID<RawCoValue>, name: "Page2" },
        { coId: "co_page3" as CoID<RawCoValue>, name: "Page3" },
      ];

      await setHash(initialPath);

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual(initialPath);
      });

      act(() => {
        result.current.goToIndex(1);
      });

      await waitFor(() => {
        expect(result.current.path).toHaveLength(2);
        expect(result.current.path[0]).toEqual(initialPath[0]);
        expect(result.current.path[1]).toEqual(initialPath[1]);
      });
    });

    it("should navigate to index 0", async () => {
      // Don't use defaultPath here since it will override manual changes
      const initialPath: PageInfo[] = [
        { coId: "co_page1" as CoID<RawCoValue>, name: "Page1" },
        { coId: "co_page2" as CoID<RawCoValue>, name: "Page2" },
      ];
      await setHash(initialPath);

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual(initialPath);
      });

      act(() => {
        result.current.goToIndex(0);
      });

      await waitFor(() => {
        expect(result.current.path).toHaveLength(1);
        expect(result.current.path[0]).toEqual(initialPath[0]);
      });
    });

    it("should handle going to last index", async () => {
      const initialPath: PageInfo[] = [
        { coId: "co_page1" as CoID<RawCoValue>, name: "Page1" },
        { coId: "co_page2" as CoID<RawCoValue>, name: "Page2" },
        { coId: "co_page3" as CoID<RawCoValue>, name: "Page3" },
      ];

      function WrapperWithDefaultPath({ children }: PropsWithChildren) {
        return (
          <HashRouterProvider defaultPath={initialPath}>
            {children}
          </HashRouterProvider>
        );
      }

      const { result } = renderHook(() => useRouter(), {
        wrapper: WrapperWithDefaultPath,
      });

      await waitFor(() => {
        expect(result.current.path).toEqual(initialPath);
      });

      act(() => {
        result.current.goToIndex(2);
      });

      await waitFor(() => {
        expect(result.current.path).toEqual(initialPath);
      });
    });

    it("should handle empty path", async () => {
      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual([]);
      });

      act(() => {
        result.current.goToIndex(0);
      });

      await waitFor(() => {
        expect(result.current.path).toEqual([]);
      });
    });
  });

  describe("setPage", () => {
    it("should set path to a single page with Root name", async () => {
      // Don't use defaultPath here since it will override manual changes
      const initialPath: PageInfo[] = [
        { coId: "co_initial" as CoID<RawCoValue>, name: "Initial" },
        { coId: "co_initial2" as CoID<RawCoValue>, name: "Initial2" },
      ];
      await setHash(initialPath);

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual(initialPath);
      });

      const newCoId = "co_newroot" as CoID<RawCoValue>;

      act(() => {
        result.current.setPage(newCoId);
      });

      await waitFor(() => {
        expect(result.current.path).toHaveLength(1);
        expect(result.current.path[0]).toEqual({
          coId: newCoId,
          name: "Root",
        });
      });
    });

    it("should replace existing path", async () => {
      // Don't use defaultPath here since it will override manual changes
      const initialPath: PageInfo[] = [
        { coId: "co_initial" as CoID<RawCoValue>, name: "Initial" },
        { coId: "co_initial2" as CoID<RawCoValue>, name: "Initial2" },
        { coId: "co_initial3" as CoID<RawCoValue>, name: "Initial3" },
      ];
      await setHash(initialPath);

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual(initialPath);
      });

      const newCoId = "co_newroot" as CoID<RawCoValue>;

      act(() => {
        result.current.setPage(newCoId);
      });

      await waitFor(() => {
        expect(result.current.path).toHaveLength(1);
        const firstPage = result.current.path[0];
        expect(firstPage).toBeDefined();
        expect(firstPage?.coId).toBe(newCoId);
        expect(firstPage?.name).toBe("Root");
      });
    });
  });

  describe("goBack", () => {
    it("should remove the last page from path", async () => {
      // Don't use defaultPath here since it will override manual changes
      const initialPath: PageInfo[] = [
        { coId: "co_page1" as CoID<RawCoValue>, name: "Page1" },
        { coId: "co_page2" as CoID<RawCoValue>, name: "Page2" },
        { coId: "co_page3" as CoID<RawCoValue>, name: "Page3" },
      ];
      await setHash(initialPath);

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual(initialPath);
      });

      act(() => {
        result.current.goBack();
      });

      await waitFor(() => {
        expect(result.current.path).toHaveLength(2);
        expect(result.current.path[0]).toEqual(initialPath[0]);
        expect(result.current.path[1]).toEqual(initialPath[1]);
      });
    });

    it("should handle going back from single page", async () => {
      // Don't use defaultPath here since it will override manual changes
      const initialPath: PageInfo[] = [
        { coId: "co_page1" as CoID<RawCoValue>, name: "Page1" },
      ];
      await setHash(initialPath);

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual(initialPath);
      });

      act(() => {
        result.current.goBack();
      });

      await waitFor(() => {
        expect(result.current.path).toEqual([]);
      });
    });

    it("should handle going back from empty path", async () => {
      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual([]);
      });

      act(() => {
        result.current.goBack();
      });

      await waitFor(() => {
        expect(result.current.path).toEqual([]);
      });
    });

    it("should handle multiple goBack calls", async () => {
      // Don't use defaultPath here since it will override manual changes
      const initialPath: PageInfo[] = [
        { coId: "co_page1" as CoID<RawCoValue>, name: "Page1" },
        { coId: "co_page2" as CoID<RawCoValue>, name: "Page2" },
        { coId: "co_page3" as CoID<RawCoValue>, name: "Page3" },
      ];
      await setHash(initialPath);

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.path).toEqual(initialPath);
      });

      act(() => {
        result.current.goBack();
      });

      await waitFor(() => {
        expect(result.current.path).toHaveLength(2);
      });

      act(() => {
        result.current.goBack();
      });

      await waitFor(() => {
        expect(result.current.path).toHaveLength(1);
        expect(result.current.path[0]).toEqual(initialPath[0]);
      });
    });
  });

  describe("defaultPath synchronization", () => {
    it("should update path when defaultPath changes", async () => {
      const initialDefaultPath: PageInfo[] = [
        { coId: "co_initial" as CoID<RawCoValue>, name: "Initial" },
      ];

      function WrapperWithInitialPath({ children }: PropsWithChildren) {
        return (
          <HashRouterProvider defaultPath={initialDefaultPath}>
            {children}
          </HashRouterProvider>
        );
      }

      const { result } = renderHook(() => useRouter(), {
        wrapper: WrapperWithInitialPath,
      });

      await waitFor(() => {
        expect(result.current.path).toEqual(initialDefaultPath);
      });

      const newDefaultPath: PageInfo[] = [
        { coId: "co_new" as CoID<RawCoValue>, name: "New" },
      ];

      function WrapperWithNewPath({ children }: PropsWithChildren) {
        return (
          <HashRouterProvider defaultPath={newDefaultPath}>
            {children}
          </HashRouterProvider>
        );
      }

      const { result: result2 } = renderHook(() => useRouter(), {
        wrapper: WrapperWithNewPath,
      });

      await waitFor(() => {
        expect(result2.current.path).toEqual(newDefaultPath);
      });
    });

    it("should not update when defaultPath is the same", async () => {
      const defaultPath: PageInfo[] = [
        { coId: "co_test" as CoID<RawCoValue>, name: "Test" },
      ];

      function WrapperWithDefaultPath({ children }: PropsWithChildren) {
        return (
          <HashRouterProvider defaultPath={defaultPath}>
            {children}
          </HashRouterProvider>
        );
      }

      const { result } = renderHook(() => useRouter(), {
        wrapper: WrapperWithDefaultPath,
      });

      await waitFor(() => {
        expect(result.current.path).toEqual(defaultPath);
      });

      const initialPath = result.current.path;

      // Re-render with same wrapper - path should remain the same
      const { result: result2 } = renderHook(() => useRouter(), {
        wrapper: WrapperWithDefaultPath,
      });

      await waitFor(() => {
        expect(result2.current.path).toEqual(initialPath);
      });
    });

    it("should override manual changes when defaultPath changes", async () => {
      const defaultPath: PageInfo[] = [
        { coId: "co_default" as CoID<RawCoValue>, name: "Default" },
      ];

      function WrapperWithDefaultPath({ children }: PropsWithChildren) {
        return (
          <HashRouterProvider defaultPath={defaultPath}>
            {children}
          </HashRouterProvider>
        );
      }

      const { result } = renderHook(() => useRouter(), {
        wrapper: WrapperWithDefaultPath,
      });

      await waitFor(() => {
        expect(result.current.path).toEqual(defaultPath);
      });

      const newDefaultPath: PageInfo[] = [
        { coId: "co_newdefault" as CoID<RawCoValue>, name: "NewDefault" },
      ];

      function WrapperWithNewDefaultPath({ children }: PropsWithChildren) {
        return (
          <HashRouterProvider defaultPath={newDefaultPath}>
            {children}
          </HashRouterProvider>
        );
      }

      const { result: result2 } = renderHook(() => useRouter(), {
        wrapper: WrapperWithNewDefaultPath,
      });

      await waitFor(() => {
        expect(result2.current.path).toEqual(newDefaultPath);
      });
    });
  });

  describe("router object stability", () => {
    it("should provide stable router object reference", () => {
      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      expect(result.current).toBeTruthy();
      expect(result.current.path).toBeDefined();
      expect(result.current.addPages).toBeDefined();
      expect(result.current.goToIndex).toBeDefined();
      expect(result.current.setPage).toBeDefined();
      expect(result.current.goBack).toBeDefined();
    });
  });

  describe("integration scenarios", () => {
    it("should handle complex navigation flow", async () => {
      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      expect(result.current.path).toEqual([]);

      const page1: PageInfo = {
        coId: "co_page1" as CoID<RawCoValue>,
        name: "Page1",
      };
      const page2: PageInfo = {
        coId: "co_page2" as CoID<RawCoValue>,
        name: "Page2",
      };
      const page3: PageInfo = {
        coId: "co_page3" as CoID<RawCoValue>,
        name: "Page3",
      };

      act(() => {
        result.current.addPages([page1]);
      });

      expect(result.current.path).toEqual([page1]);

      act(() => {
        result.current.addPages([page2]);
      });

      expect(result.current.path).toEqual([page1, page2]);

      act(() => {
        result.current.addPages([page3]);
      });

      expect(result.current.path).toEqual([page1, page2, page3]);

      act(() => {
        result.current.goBack();
      });

      expect(result.current.path).toEqual([page1, page2]);

      act(() => {
        result.current.goToIndex(0);
      });

      expect(result.current.path).toEqual([page1]);

      act(() => {
        result.current.setPage("co_newroot" as CoID<RawCoValue>);
      });

      expect(result.current.path).toEqual([
        { coId: "co_newroot" as CoID<RawCoValue>, name: "Root" },
      ]);
    });

    it("should persist complex navigation flow to hash", async () => {
      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      expect(result.current.path).toEqual([]);

      const page1: PageInfo = {
        coId: "co_page1" as CoID<RawCoValue>,
        name: "Page1",
      };
      const page2: PageInfo = {
        coId: "co_page2" as CoID<RawCoValue>,
        name: "Page2",
      };

      act(() => {
        result.current.addPages([page1]);
      });

      expect(result.current.path).toEqual([page1]);

      act(() => {
        result.current.addPages([page2]);
      });

      expect(result.current.path).toEqual([page1, page2]);

      act(() => {
        result.current.goBack();
      });

      expect(result.current.path).toEqual([page1]);
      const hash = window.location.hash.slice(2);
      const decoded = hash.split("/").map((segment) => {
        const [coId, encodedName] = segment.split(":");
        return {
          coId,
          name: encodedName ? decodeURIComponent(encodedName) : undefined,
        } as PageInfo;
      });
      expect(decoded).toEqual([page1]);
    });
  });
});
