// #region Imports
import { co, z } from 'jazz-tools';
// import { JazzBrowserContextManager } from 'jazz-tools/browser';
// #endregion

// #region InlineSchema
const ToDo = co.map({ title: z.string(), completed: z.boolean() });
const ToDoList = co.list(ToDo);
// #endregion

// #region SimpleRouting
// @ts-expect-error I'll redeclare this in a separate section
//[!code --:2]
const newList = ToDoList.create([{ title: 'Learn Jazz', completed: false }]);
// @ts-expect-error I'll redeclare this in a separate section
const listId = newList.$jazz.id;

// [!code ++:8]
// @ts-expect-error I'll redeclare this in a separate section
const listId = new URLSearchParams(window.location.search).get('id');

if (!listId) {
  const newList = ToDoList.create([{ title: 'Learn Jazz', completed: false }]);
  await newList.$jazz.waitForSync();
  window.location.search = `?id=${newList.$jazz.id}`;
  throw new Error('Redirecting...');
}
// #endregion

// #region Context
// @ts-expect-error: this actually creates a new context?! I broke the import, but...
await new JazzBrowserContextManager().createContext({
  sync: {
    peer: 'wss://cloud.jazz.tools?key=minimal-vanilla-example',
    when: 'always',
  },
});

// @ts-expect-error I'll redeclare this
const newList = ToDoList.create([{ title: 'Learn Jazz', completed: false }]);
// @ts-expect-error I'll redeclare this
const listId = newList.$jazz.id;
// #endregion