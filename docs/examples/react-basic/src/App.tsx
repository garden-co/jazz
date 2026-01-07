import { JazzProvider } from '@jazz/react';
import { TaskList } from './components/TaskList';
import { TaskForm } from './components/TaskForm';
import { useJazzClient } from './client';

export default function App() {
  const client = useJazzClient();

  if (!client) {
    return <div>Loading Jazz...</div>;
  }

  return (
    <JazzProvider value={client}>
      <main className="container mx-auto p-4">
        <h1 className="text-2xl font-bold mb-4">Task Manager</h1>
        <TaskForm />
        <TaskList />
      </main>
    </JazzProvider>
  );
}
