// #region Imports
import { co, z } from 'jazz-tools';
import { JazzBrowserContextManager } from 'jazz-tools/browser';
// #endregion

// #region InlineSchema
const ToDo = co.map({ title: z.string(), completed: z.boolean() });
const ToDoList = co.list(ToDo);
// #endregion

// #region ContextManager
await new JazzBrowserContextManager().createContext({
  sync: {
    peer: 'wss://cloud.jazz.tools?key=minimal-vanilla-example',
    when: 'always',
  },
});
// #endregion

// #region SimpleRouting
const listId = new URLSearchParams(window.location.search).get('id');

if (!listId) {
  const newList = ToDoList.create([{ title: 'Learn Jazz', completed: false }]);
  await newList.$jazz.waitForSync();
  window.location.search = `?id=${newList.$jazz.id}`;
  throw new Error('Redirecting...');
}
// #endregion

// #region BuildUI
const app = document.querySelector('#app')!;
const id = Object.assign(document.createElement('small'), {
  innerText: `List ID: ${listId}`,
});
const listContainer = document.createElement('div');
app.append(listContainer, id);
// #endregion

// #region toDoItemElement
function toDoItemElement(todo: co.loaded<typeof ToDo>) {
  const label = document.createElement('label');
  const checkbox = Object.assign(document.createElement('input'), {
    type: 'checkbox',
    checked: todo.completed,
    onclick: () => todo.$jazz.set('completed', checkbox.checked),
  });
  label.append(checkbox, todo.title);
  return label;
}
// #endregion

// #region newToDoFormElement
function newToDoFormElement(list: co.loaded<typeof ToDoList>) {
  const form = Object.assign(document.createElement('form'), {
    onsubmit: (e: Event) => {
      e.preventDefault();
      list.$jazz.push({ title: input.value, completed: false });
    }
  });
  const input = Object.assign(document.createElement('input'), {
    placeholder: 'New task',
  });
  const btn = Object.assign(document.createElement('button'), {
    innerText: 'Add',
  });
  form.append(input, btn);
  return form;
}
// #endregion

// #region SubscribeToChanges
const unsubscribe = ToDoList.subscribe(
  listId,
  { resolve: { $each: true } },
  (toDoList) => {
    const addForm = newToDoFormElement(toDoList);
    listContainer.replaceChildren(
      ...toDoList.map((todo) => {
        return toDoItemElement(todo);
      }),
      addForm
    );
  }
);
// #endregion
