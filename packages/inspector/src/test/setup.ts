const dialogPrototype =
  globalThis.HTMLDialogElement?.prototype ?? globalThis.HTMLElement?.prototype;

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
