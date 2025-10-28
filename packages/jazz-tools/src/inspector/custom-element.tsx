import React from "react";
import { setup } from "goober";
import { Account } from "jazz-tools";
import { createRoot } from "react-dom/client";
import { JazzInspectorInternal } from "./viewer/new-app.js";

setup(React.createElement);

export class JazzInspectorElement extends HTMLElement {
  private root: ReturnType<typeof createRoot> | null = null;

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
    this.root = createRoot(this);
    this.startAccountPolling();
    this.render();
  }

  disconnectedCallback() {
    this.root?.unmount();
    this.root = null;
    this.stopAccountPolling();
  }

  private render() {
    if (!this.account) {
      return;
    }

    this.root?.render(
      <JazzInspectorInternal
        localNode={this.account.$jazz.localNode}
        accountId={this.account.$jazz.raw.id}
      />,
    );
  }
}

customElements.define("jazz-inspector", JazzInspectorElement);
