import { useState } from "react";
import { useMutate } from "@jazz/react";
import { app } from "../generated/client.js";

//#region task-form
interface TaskFormProps {
  projectId: string;
}

export function TaskForm({ projectId }: TaskFormProps) {
  const [title, setTitle] = useState("");
  const [description, setDescription] = useState("");
  const mutate = useMutate(app.tasks);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();

    const id = mutate.create({
      title,
      description: description || null,
      status: "open",
      priority: "medium",
      project: projectId,
      createdAt: BigInt(Date.now()),
      updatedAt: BigInt(Date.now()),
      isCompleted: false,
    });

    console.log("Created task:", id);

    // Reset form
    setTitle("");
    setDescription("");
  };

  return (
    <form onSubmit={handleSubmit}>
      <div>
        <label htmlFor="title">Title</label>
        <input
          id="title"
          type="text"
          value={title}
          onChange={e => setTitle(e.target.value)}
          required
        />
      </div>
      <div>
        <label htmlFor="description">Description</label>
        <textarea
          id="description"
          value={description}
          onChange={e => setDescription(e.target.value)}
        />
      </div>
      <button type="submit">Create Task</button>
    </form>
  );
}
//#endregion
