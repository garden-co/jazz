import { useEffect, useState } from 'react';
import { useJazz } from '@jazz/react';
import { app } from '../generated/client';
import type { Task } from '../generated/types';

// Manual subscription control for advanced use cases
export function useTaskSubscription(taskId: string | null) {
  const db = useJazz();
  const [task, setTask] = useState<Task | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!taskId) {
      setTask(null);
      setLoading(false);
      return;
    }

    setLoading(true);

    // Manual subscription - you control when to subscribe/unsubscribe
    const unsubscribe = app.tasks.subscribe(db, taskId, (row) => {
      setTask(row);
      setLoading(false);
    });

    // Cleanup on unmount or when taskId changes
    return unsubscribe;
  }, [db, taskId]);

  return { task, loading };
}
