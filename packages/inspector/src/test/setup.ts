const dialogPrototype =
  globalThis.HTMLDialogElement?.prototype ?? globalThis.HTMLElement?.prototype;

function createMemoryStorage(): Storage {
  const store = new Map<string, string>();
  return {
    get length() {
      return store.size;
    },
    clear() {
      store.clear();
    },
    getItem(key: string) {
      return store.get(key) ?? null;
    },
    key(index: number) {
      return Array.from(store.keys())[index] ?? null;
    },
    removeItem(key: string) {
      store.delete(key);
    },
    setItem(key: string, value: string) {
      store.set(key, value);
    },
  };
}

if (typeof globalThis.localStorage?.clear !== "function") {
  const storage = createMemoryStorage();
  Object.defineProperty(globalThis, "localStorage", {
    configurable: true,
    enumerable: true,
    value: storage,
  });
  if (globalThis.window) {
    Object.defineProperty(globalThis.window, "localStorage", {
      configurable: true,
      enumerable: true,
      value: storage,
    });
  }
}

if (typeof globalThis.confirm !== "function") {
  globalThis.confirm = () => false;
}

if (dialogPrototype) {
  if (!("open" in dialogPrototype)) {
    Object.defineProperty(dialogPrototype, "open", {
      configurable: true,
      enumerable: true,
      get() {
        return this.hasAttribute("open");
      },
      set(value: boolean) {
        if (value) {
          this.setAttribute("open", "");
        } else {
          this.removeAttribute("open");
        }
      },
    });
  }

  if (typeof dialogPrototype.showModal !== "function") {
    dialogPrototype.showModal = function showModal() {
      this.setAttribute("open", "");
    };
  }

  if (typeof dialogPrototype.close !== "function") {
    dialogPrototype.close = function close() {
      const wasOpen = this.hasAttribute("open");
      this.removeAttribute("open");
      if (wasOpen) {
        this.dispatchEvent(new Event("close"));
      }
    };
  }
}
