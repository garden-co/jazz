import {
  useAccount,
  useCoState,
  useCoStateWithSelector,
} from "jazz-tools/react-core";
import { useRef, useState } from "react";
import { useNavigate, useParams } from "react-router";
import {
  MAX_PRIORITY,
  MIN_PRIORITY,
  Task,
  TodoAccount,
  TodoProject,
} from "./1_schema";
import { OrderByDirection } from "jazz-tools";

export function ProjectScreen() {
  const { projectId } = useParams();
  const [visibleTasks, setVisibleTasks] = useState(20);
  const project = useCoState(TodoProject, projectId, {
    resolve: {
      tasks: {
        $orderBy: { priority: OrderByDirection.DESC },
        $limit: visibleTasks,
      },
    },
  });
  const totalTaskCount = useCoStateWithSelector(TodoProject, projectId, {
    resolve: {
      tasks: true,
    },
    select: (project) => project?.tasks.length ?? 0,
  });
  const { me } = useAccount(TodoAccount, {
    resolve: {
      root: true,
    },
  });
  const navigate = useNavigate();

  const firstRenderMarker = useRef(false);
  const loadedMarker = useRef(false);

  if (!firstRenderMarker.current) {
    if (me?.root.profilingEnabled) {
      console.profile(projectId);
    }

    firstRenderMarker.current = true;
    performance.mark(`${projectId}-start`);
  }

  if (!project) return null;

  if (!loadedMarker.current) {
    loadedMarker.current = true;
    performance.mark(`${projectId}-loaded`);
    console.log(
      performance.measure(
        `Loading ${projectId}`,
        `${projectId}-start`,
        `${projectId}-loaded`,
      ),
    );
    if (me?.root.profilingEnabled) {
      console.profileEnd(project.$jazz.id);
    }
  }

  return (
    <div
      style={{
        maxWidth: "800px",
        margin: "0 auto",
        padding: "20px",
        fontFamily:
          '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          marginBottom: "30px",
          paddingBottom: "20px",
          borderBottom: "2px solid #f0f0f0",
        }}
      >
        <h1
          style={{
            margin: 0,
            fontSize: "2.5rem",
            fontWeight: "700",
            color: "#2c3e50",
          }}
        >
          {totalTaskCount} tasks
        </h1>
        <button
          onClick={() => navigate("/")}
          style={{
            padding: "12px 24px",
            backgroundColor: "#6c757d",
            color: "white",
            border: "none",
            borderRadius: "8px",
            fontSize: "16px",
            cursor: "pointer",
            transition: "background-color 0.2s ease",
            fontWeight: "500",
          }}
        >
          Back
        </button>
      </div>

      <div
        style={{
          display: "flex",
          flexDirection: "column",
          gap: "12px",
          marginBottom: "30px",
        }}
      >
        {[...project.tasks.$jazz.refs].map((taskRef) => (
          <TaskRow key={taskRef.id} taskId={taskRef.id} />
        ))}
      </div>

      {visibleTasks < totalTaskCount && (
        <div
          style={{
            textAlign: "center",
          }}
        >
          <button
            onClick={() => setVisibleTasks(visibleTasks + 20)}
            style={{
              padding: "14px 32px",
              backgroundColor: "#007bff",
              color: "white",
              border: "none",
              borderRadius: "10px",
              fontSize: "16px",
              cursor: "pointer",
              transition: "all 0.2s ease",
              fontWeight: "600",
              boxShadow: "0 4px 12px rgba(0, 123, 255, 0.3)",
            }}
          >
            Load more
          </button>
        </div>
      )}
    </div>
  );
}

function TaskRow({ taskId }: { taskId: string }) {
  const task = useCoState(Task, taskId, {
    resolve: {
      text: true,
    },
  });
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        padding: "16px 20px",
        backgroundColor: "white",
        borderRadius: "12px",
        boxShadow: "0 2px 8px rgba(0, 0, 0, 0.1)",
        border: "1px solid #e9ecef",
        transition: "all 0.2s ease",
        cursor: "pointer",
        gap: "16px",
      }}
      onMouseOver={(e) => {
        e.currentTarget.style.transform = "translateY(-2px)";
        e.currentTarget.style.boxShadow = "0 4px 16px rgba(0, 0, 0, 0.15)";
      }}
      onMouseOut={(e) => {
        e.currentTarget.style.transform = "translateY(0)";
        e.currentTarget.style.boxShadow = "0 2px 8px rgba(0, 0, 0, 0.1)";
      }}
    >
      <input
        type="checkbox"
        checked={task?.done}
        onChange={(e) => {
          if (task) task.$jazz.set("done", e.target.checked);
        }}
        style={{
          width: "20px",
          height: "20px",
          accentColor: "#28a745",
          cursor: "pointer",
        }}
      />
      <span
        style={{
          fontSize: "16px",
          color: task?.done ? "#6c757d" : "#2c3e50",
          textDecoration: task?.done ? "line-through" : "none",
          flex: 1,
          fontWeight: task?.done ? "400" : "500",
          transition: "all 0.2s ease",
        }}
      >
        {task?.text}
      </span>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: "8px",
        }}
      >
        <label
          style={{
            fontSize: "14px",
            color: "#6c757d",
            fontWeight: "500",
          }}
        >
          Priority:
        </label>
        <input
          type="number"
          value={task?.priority ?? MIN_PRIORITY}
          onChange={(e) => {
            if (task) {
              const newPriority = parseInt(e.target.value) || MIN_PRIORITY;
              task.$jazz.set("priority", newPriority);
            }
          }}
          style={{
            width: "70px",
            padding: "6px 8px",
            fontSize: "14px",
            border: "1px solid #ddd",
            borderRadius: "6px",
            textAlign: "center",
            backgroundColor: "#f8f9fa",
          }}
          min={MIN_PRIORITY}
          max={MAX_PRIORITY}
        />
      </div>
    </div>
  );
}
