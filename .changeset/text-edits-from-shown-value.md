---
"jazz-tools": patch
---

Two people typing into one text-merge column at once no longer lose characters. `db.update` takes `editedFrom: { column: value }`, the value an editor last showed or wrote; when a merged value reached the local store before the editor showed it, the write is merged over that value instead of undoing it. `markdownSync`'s `onChange` now receives `{ editedFrom }` as its second argument; pass it on: `onChange: (body, { editedFrom }) => db.update(app.notes, id, { body }, { editedFrom: { body: editedFrom } })`. Merges also keep both sides' letters when two people type into the same word.
