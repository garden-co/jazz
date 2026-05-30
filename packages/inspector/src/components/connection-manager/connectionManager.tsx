import { useNavigate } from "@tanstack/react-router";
import { useStandaloneConnection } from "#contexts/standalone-connection-context";
import { appRoutes } from "#lib/navigation/appRoutes";
import styles from "../../App.module.css";

export function ConnectionManager(): React.ReactElement {
  const { connections, activeConnectionId, deleteConnection, openConnection } =
    useStandaloneConnection();
  const navigate = useNavigate();

  return (
    <main className={styles.connectionPage}>
      <section className={styles.connectionManager}>
        <div className={styles.managerHeader}>
          <div>
            <h2 className={styles.stateTitle}>Connections</h2>
            <p className={styles.managerSubtitle}>Saved standalone Jazz server connections.</p>
          </div>
          <div className={styles.actionRow}>
            <button
              type="button"
              onClick={() => navigate({ to: appRoutes.newConnection })}
              className={styles.actionButton}
            >
              Add connection
            </button>
          </div>
        </div>
        {connections.length === 0 ? (
          <p className={styles.emptyText}>No saved connections.</p>
        ) : (
          <div className={styles.connectionList}>
            {connections.map((connection) => {
              const isActive = connection.id === activeConnectionId;

              return (
                <article key={connection.id} className={styles.connectionItem}>
                  <div className={styles.connectionDetails}>
                    <div className={styles.connectionTitleRow}>
                      <h3 className={styles.connectionName}>{connection.name}</h3>
                      {isActive ? <span className={styles.activeBadge}>Active</span> : null}
                    </div>
                    <p className={styles.connectionMeta}>{connection.serverUrl}</p>
                    <p className={styles.connectionMeta}>
                      {connection.appId} · {connection.env}/{connection.branch}
                    </p>
                  </div>
                  <div className={styles.connectionActions}>
                    <button
                      type="button"
                      onClick={() => void openConnection(connection.id)}
                      className={styles.actionButton}
                      aria-label={`Open ${connection.name}`}
                    >
                      Open
                    </button>
                    <button
                      type="button"
                      onClick={() =>
                        navigate({
                          to: appRoutes.editConnection,
                          params: { connectionId: connection.id },
                        })
                      }
                      className={styles.actionButtonSecondary}
                      aria-label={`Edit ${connection.name}`}
                    >
                      Edit
                    </button>
                    <button
                      type="button"
                      onClick={() => deleteConnection(connection.id)}
                      className={styles.actionButtonSecondary}
                      aria-label={`Delete ${connection.name}`}
                    >
                      Delete
                    </button>
                  </div>
                </article>
              );
            })}
          </div>
        )}
      </section>
    </main>
  );
}
