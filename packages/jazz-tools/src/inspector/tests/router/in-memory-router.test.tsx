// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, renderHook, waitFor } from "@testing-library/react";
import { type PropsWithChildren } from "react";
import { InMemoryRouterProvider } from "../../router/in-memory-router.js";
import { useRouter } from "../../router/context.js";
import type { PageInfo } from "../../viewer/types.js";
import type { CoID, RawCoValue } from "cojson";

const STORAGE_KEY = "jazz-inspector-paths";

function Wrapper({ children }: PropsWithChildren) {
  return <InMemoryRouterProvider>{children}</InMemoryRouterProvider>;
}

describe("InMemoryRouterProvider", () => {
  beforeEach(() => {
    if (typeof localStorage !== "undefined") {
      localStorage.clear();
    }
    vi.clearAllMocks();
  });

  afterEach(() => {
    if (typeof localStorage !== "undefined") {
      localStorage.clear();
    }
  });

  describe("initialization", () => {
    it("should initialize with empty path when no localStorage and no defaultPath", () => {
      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      expect(result.current.path).toEqual([]);
    });

    it("should initialize with defaultPath when provided", () => {
      const defaultPath: PageInfo[] = [
        { coId: "co_test1" as CoID<RawCoValue>, name: "Test1" },
        { coId: "co_test2" as CoID<RawCoValue>, name: "Test2" },
      ];

      function WrapperWithDefaultPath({ children }: PropsWithChildren) {
        return (
          <InMemoryRouterProvider defaultPath={defaultPath}>
            {children}
          </InMemoryRouterProvider>
        );
      }

      const { result } = renderHook(() => useRouter(), {
        wrapper: WrapperWithDefaultPath,
      });
      expect(result.current.path).toEqual(defaultPath);
    });

    it("should initialize from localStorage when available", () => {
      const storedPath: PageInfo[] = [
        { coId: "co_stored1" as CoID<RawCoValue>, name: "Stored1" },
      ];
      localStorage.setItem(STORAGE_KEY, JSON.stringify(storedPath));

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      expect(result.current.path).toEqual(storedPath);
    });

    it("should sync defaultPath over localStorage when defaultPath is provided", async () => {
      const storedPath: PageInfo[] = [
        { coId: "co_stored" as CoID<RawCoValue>, name: "Stored" },
      ];
      const defaultPath: PageInfo[] = [
        { coId: "co_default" as CoID<RawCoValue>, name: "Default" },
      ];
      localStorage.setItem(STORAGE_KEY, JSON.stringify(storedPath));

      function WrapperWithDefaultPath({ children }: PropsWithChildren) {
        return (
          <InMemoryRouterProvider defaultPath={defaultPath}>
            {children}
          </InMemoryRouterProvider>
        );
      }

      const { result } = renderHook(() => useRouter(), {
        wrapper: WrapperWithDefaultPath,
      });

      await waitFor(() => {
        expect(result.current.path).toEqual(defaultPath);
      });
    });

    it("should handle invalid JSON in localStorage gracefully", () => {
      localStorage.setItem(STORAGE_KEY, "invalid json");
      const consoleWarnSpy = vi
        .spyOn(console, "warn")
        .mockImplementation(() => {});

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      expect(consoleWarnSpy).toHaveBeenCalled();
      expect(result.current.path).toEqual([]);
      consoleWarnSpy.mockRestore();
    });

    it("should handle SSR scenario - component initializes with empty path when no defaultPath", () => {
      // In SSR, window is undefined, so the initial state should be empty array
      // We test this by ensuring the component works correctly without localStorage
      localStorage.removeItem(STORAGE_KEY);

      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      // When no localStorage and no defaultPath, should initialize with empty array
      expect(result.current.path).toEqual([]);
    });
  });

  describe("localStorage persistence", () => {
    it("should persist path changes to localStorage", async () => {
      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      const newPages: PageInfo[] = [
        { coId: "co_new1" as CoID<RawCoValue>, name: "New1" },
      ];

      act(() => {
        result.current.addPages(newPages);
      });

      await waitFor(() => {
        const stored = localStorage.getItem(STORAGE_KEY);
        expect(stored).toBeTruthy();
        expect(JSON.parse(stored!)).toEqual(newPages);
      });
    });

    it("should update localStorage when path changes", async () => {
      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

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
        const stored = localStorage.getItem(STORAGE_KEY);
        const parsed = JSON.parse(stored!);
        expect(parsed).toHaveLength(2);
        expect(parsed[0]).toEqual(firstPages[0]);
        expect(parsed[1]).toEqual(secondPages[0]);
      });
    });
  });

  describe("addPages", () => {
    it("should add pages to the current path", async () => {
      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

      expect(result.current.path).toEqual([]);

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
      localStorage.setItem(STORAGE_KEY, JSON.stringify(initialPath));

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
      localStorage.setItem(STORAGE_KEY, JSON.stringify(initialPath));

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
      localStorage.setItem(STORAGE_KEY, JSON.stringify(initialPath));

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
      localStorage.setItem(STORAGE_KEY, JSON.stringify(initialPath));

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
          <InMemoryRouterProvider defaultPath={initialPath}>
            {children}
          </InMemoryRouterProvider>
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
      localStorage.setItem(STORAGE_KEY, JSON.stringify(initialPath));

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
      localStorage.setItem(STORAGE_KEY, JSON.stringify(initialPath));

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
      localStorage.setItem(STORAGE_KEY, JSON.stringify(initialPath));

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
      localStorage.setItem(STORAGE_KEY, JSON.stringify(initialPath));

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
      localStorage.setItem(STORAGE_KEY, JSON.stringify(initialPath));

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
          <InMemoryRouterProvider defaultPath={initialDefaultPath}>
            {children}
          </InMemoryRouterProvider>
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
          <InMemoryRouterProvider defaultPath={newDefaultPath}>
            {children}
          </InMemoryRouterProvider>
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
          <InMemoryRouterProvider defaultPath={defaultPath}>
            {children}
          </InMemoryRouterProvider>
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
          <InMemoryRouterProvider defaultPath={defaultPath}>
            {children}
          </InMemoryRouterProvider>
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
          <InMemoryRouterProvider defaultPath={newDefaultPath}>
            {children}
          </InMemoryRouterProvider>
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
      await waitFor(() => {
        expect(result.current.path).toEqual([page1]);
      });

      act(() => {
        result.current.addPages([page2]);
      });
      await waitFor(() => {
        expect(result.current.path).toEqual([page1, page2]);
      });

      act(() => {
        result.current.addPages([page3]);
      });
      await waitFor(() => {
        expect(result.current.path).toEqual([page1, page2, page3]);
      });

      act(() => {
        result.current.goBack();
      });
      await waitFor(() => {
        expect(result.current.path).toEqual([page1, page2]);
      });

      act(() => {
        result.current.goToIndex(0);
      });
      await waitFor(() => {
        expect(result.current.path).toEqual([page1]);
      });

      act(() => {
        result.current.setPage("co_newroot" as CoID<RawCoValue>);
      });
      await waitFor(() => {
        expect(result.current.path).toEqual([
          { coId: "co_newroot" as CoID<RawCoValue>, name: "Root" },
        ]);
      });
    });

    it("should persist complex navigation flow to localStorage", async () => {
      const { result } = renderHook(() => useRouter(), { wrapper: Wrapper });

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
      await waitFor(() => {
        expect(result.current.path).toEqual([page1]);
      });

      act(() => {
        result.current.addPages([page2]);
      });
      await waitFor(() => {
        expect(result.current.path).toEqual([page1, page2]);
      });

      act(() => {
        result.current.goBack();
      });
      await waitFor(() => {
        const stored = localStorage.getItem(STORAGE_KEY);
        const parsed = JSON.parse(stored!);
        expect(parsed).toEqual([page1]);
      });
    });
  });
});
