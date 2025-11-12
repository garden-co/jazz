"use client";

import { JazzAccount } from "@/schema";
import { CoPlainText, ExportedCoValue } from "jazz-tools";
import { useAgent, useCoState } from "jazz-tools/react-core";

export function TodoList({
  id,
  preloaded,
}: {
  id: string;
  preloaded?: ExportedCoValue;
}) {
  const me = useAgent();
  const list = useCoState(JazzAccount, id, {
    preloaded,
    select: (account) => {
      if (!account.$isLoaded) return account;

      return account.profile.todos;
    },
  });

  if (!list.$isLoaded) return <div>{list.$jazz.loadingState}</div>;

  const handleAddTodo = () => {
    list.$jazz.push("");
  };

  const canAddTodo = me.canWrite(list);

  return (
    <div className="min-w-[300px] flex flex-col gap-4">
      <h1 className="text-2xl font-bold flex items-center gap-2">
        Todo List
        <button
          onClick={handleAddTodo}
          className={
            "text-sm border-2 border-gray-300 rounded-md px-2 py-1 transition-opacity " +
            (canAddTodo ? "cursor-pointer opacity-100" : "opacity-0")
          }
        >
          Add todo
        </button>
      </h1>
      <ol>
        {list.map((todo, index) => (
          <li key={todo.$jazz.id} className="group flex items-center gap-2">
            <span className="font-mono">{index + 1}.</span>
            <TodoItem
              todo={todo}
              canEdit={me.canWrite(todo)}
              canDelete={me.canWrite(list)}
              onDelete={() => {
                if (!me.canWrite(list)) return;
                list.$jazz.remove(index);
              }}
            />
          </li>
        ))}
      </ol>
    </div>
  );
}

function TodoItem({
  todo,
  canEdit,
  canDelete,
  onDelete,
}: {
  todo: CoPlainText;
  canEdit: boolean;
  canDelete: boolean;
  onDelete: () => void;
}) {
  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (!canEdit) return;
    todo.$jazz.applyDiff(e.target.value);
  };

  const text = todo.toString();

  return (
    <>
      <input
        type="text"
        className="font-mono flex-1"
        size={Math.max(text.length, 1)}
        disabled={!canEdit}
        value={text}
        onChange={handleChange}
      />
      {canDelete && (
        <button
          type="button"
          onClick={onDelete}
          className="opacity-0 group-hover:opacity-100 focus-visible:opacity-100 transition-opacity text-xs text-red-600 hover:text-red-800 ml-2"
          aria-label="Delete todo"
        >
          Delete
        </button>
      )}
    </>
  );
}
