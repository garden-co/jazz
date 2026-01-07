import { useState } from 'react';
import { useMutate } from '@jazz/react';
import { app } from '../generated/client';

export function TaskForm() {
  const [title, setTitle] = useState('');
  const [priority, setPriority] = useState('medium');

  // Get mutation functions without subscribing to data
  const mutate = useMutate(app.tasks);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();

    if (!title.trim()) return;

    // Create a new task - updates are instant, sync happens in background
    mutate.create({
      title: title.trim(),
      completed: false,
      priority,
      createdAt: BigInt(Date.now()),
    });

    setTitle('');
  };

  return (
    <form onSubmit={handleSubmit} className="flex gap-2 mb-4">
      <input
        type="text"
        value={title}
        onChange={(e) => setTitle(e.target.value)}
        placeholder="New task..."
        className="flex-1 px-3 py-2 border rounded"
      />
      <select
        value={priority}
        onChange={(e) => setPriority(e.target.value)}
        className="px-3 py-2 border rounded"
      >
        <option value="low">Low</option>
        <option value="medium">Medium</option>
        <option value="high">High</option>
      </select>
      <button
        type="submit"
        className="px-4 py-2 bg-blue-500 text-white rounded"
      >
        Add
      </button>
    </form>
  );
}
