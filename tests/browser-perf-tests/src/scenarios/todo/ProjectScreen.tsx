import { useAccount, useCoState, useCoStates } from "jazz-tools/react-core";
import { useRef, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { AppAccount, TodoProject, Task } from "../../schema";

export function ProjectScreen() {
  const { projectId } = useParams();
  const [visibleTasks, setVisibleTasks] = useState(20);
  const project = useCoState(TodoProject, projectId, {
    resolve: {
      tasks: true,
    },
  });
  const taskIds = project.$isLoaded
    ? Array.from(project.tasks.$jazz.refs)
        .slice(0, visibleTasks)
        .map((ref) => ref.id)
    : [];
  const tasks = useCoStates(Task, taskIds, {
    resolve: {
      text: true,
    },
  });
  const me = useAccount(AppAccount, {
    resolve: {
      root: true,
    },
  });

  const profilingEnabled = me.$isLoaded && me.root.profilingEnabled;
  const firstRenderMarker = useRef(false);
  if (!firstRenderMarker.current) {
    if (profilingEnabled) {
      console.profile(projectId);
    }

    firstRenderMarker.current = true;
    performance.mark(`${projectId}-start`);
  }

  const loadedMarker = useRef(false);
  if (!loadedMarker.current && project) {
    loadedMarker.current = true;
    performance.mark(`${projectId}-loaded`);
    console.log(
      performance.measure(
        `Loading ${projectId}`,
        `${projectId}-start`,
        `${projectId}-loaded`,
      ),
    );
    if (profilingEnabled) {
      console.profileEnd(project.$jazz.id);
    }
  }

  const navigate = useNavigate();

  if (!project.$isLoaded) return null;

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
          {project.tasks.length} tasks
        </h1>
        <button
          onClick={() => navigate("/todo")}
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
        {tasks
          .filter((task) => task.$isLoaded)
          .map((task, index) => (
            <label
              key={task?.$jazz.id ?? index}
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
              }}
              onMouseOver={(e) => {
                e.currentTarget.style.transform = "translateY(-2px)";
                e.currentTarget.style.boxShadow =
                  "0 4px 16px rgba(0, 0, 0, 0.15)";
              }}
              onMouseOut={(e) => {
                e.currentTarget.style.transform = "translateY(0)";
                e.currentTarget.style.boxShadow =
                  "0 2px 8px rgba(0, 0, 0, 0.1)";
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
                  marginRight: "16px",
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
            </label>
          ))}
      </div>

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
    </div>
  );
}
