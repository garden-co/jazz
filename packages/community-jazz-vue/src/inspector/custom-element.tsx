import { Account } from "jazz-tools";
import { type App, createApp } from "vue";
import { JazzInspectorInternal } from "./viewer/new-app.js";

export class JazzInspectorElement extends HTMLElement {
  private app: App | null = null;

  account: Account | null = null;

  private interval: ReturnType<typeof setInterval> | undefined;

  loadAccount() {
    try {
      const value = Account.getMe();

      if (value !== this.account) {
        this.account = value;
        this.render();
      }
    } catch {}
  }

  startAccountPolling() {
    if (this.interval) return;

    this.loadAccount();

    this.interval = setInterval(() => {
      this.loadAccount();
    }, 1000);
  }

  stopAccountPolling() {
    if (this.interval) clearInterval(this.interval);
  }

  connectedCallback() {
    this.startAccountPolling();
    this.render();
  }

  disconnectedCallback() {
    if (this.app) {
      this.app.unmount();
      this.app = null;
    }
    this.stopAccountPolling();
  }

  private render() {
    if (!this.account) {
      return;
    }

    if (this.app) {
      this.app.unmount();
    }

    this.app = createApp(JazzInspectorInternal, {
      localNode: this.account._raw.core.node,
      accountId: this.account._raw.id,
    });

    this.app.mount(this);
  }
}

customElements.define("jazz-inspector", JazzInspectorElement);
