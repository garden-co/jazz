const dialogPrototype =
  globalThis.HTMLDialogElement?.prototype ?? globalThis.HTMLElement?.prototype;

const localStorageValues = new Map<string, string>();
Object.defineProperty(globalThis, "localStorage", {
  configurable: true,
  value: {
    clear: () => localStorageValues.clear(),
    getItem: (key: string) => localStorageValues.get(key) ?? null,
    key: (index: number) => [...localStorageValues.keys()][index] ?? null,
    get length() {
      return localStorageValues.size;
    },
    removeItem: (key: string) => localStorageValues.delete(key),
    setItem: (key: string, value: string) => localStorageValues.set(key, value),
  } satisfies Storage,
});

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
