const dialogPrototype =
  globalThis.HTMLDialogElement?.prototype ?? globalThis.HTMLElement?.prototype;

if (typeof globalThis.localStorage?.clear !== "function") {
  const entries = new Map<string, string>();
  Object.defineProperty(globalThis, "localStorage", {
    configurable: true,
    value: {
      get length() {
        return entries.size;
      },
      clear() {
        entries.clear();
      },
      getItem(key: string) {
        return entries.get(key) ?? null;
      },
      key(index: number) {
        return Array.from(entries.keys())[index] ?? null;
      },
      removeItem(key: string) {
        entries.delete(key);
      },
      setItem(key: string, value: string) {
        entries.set(key, String(value));
      },
    },
  });
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
