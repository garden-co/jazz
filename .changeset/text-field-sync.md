---
"jazz-tools": patch
---

New `textFieldSync` in `jazz-tools/text-field` keeps a plain `<textarea>` or `<input>` in step with a text merge column. It writes each change with `editedFrom`, and applies merged values as a change, so the cursor stays where it was and no keystroke is lost when several people type at once. The `<textarea>` snippet in the collaborative text editing recipe used to lose characters and move the cursor to the end; it now uses `textFieldSync`, with an uncontrolled field (`defaultValue`).
