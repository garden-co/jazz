---
"jazz-tools": patch
---

A visible browser tab now notices when its shared worker dies, for example when another tab crashes, even if the tab has nothing pending. Before, such a tab stayed "connecting" until it wrote something, was hidden and shown again, or was reloaded, and the writes the dead worker still held waited for that too. Visible tabs now check the worker every 30 seconds and reopen it when it doesn't answer; hidden tabs still rest until they are shown.
