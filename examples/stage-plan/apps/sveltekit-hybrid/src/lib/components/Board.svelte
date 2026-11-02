<script lang="ts">
  import { getDb, QuerySubscription } from "jazz-tools/svelte";
  import { app, TASK_STATUSES, type Show, type Task, type TaskStatus } from "../../../schema";
  import { addTask, moveTask, rankAfter, STATUS_LABELS, type Me } from "$lib/model/actions";

  let { show, me }: { show: Show; me: Me } = $props();
  const db = getDb();
  const tasks = new QuerySubscription(() => app.tasks.where({ showId: show.id }));
  let title = $state("");

  const column = (status: TaskStatus) =>
    (tasks.current ?? [])
      .filter((task) => task.status === status)
      .sort((a, b) => a.rank - b.rank);

  async function add(event: SubmitEvent) {
    event.preventDefault();
    const trimmed = title.trim();
    if (!trimmed) return;
    title = "";
    await addTask(db, me, show.id, trimmed, "todo", rankAfter(column("todo")));
  }

  function move(task: Task, status: TaskStatus) {
    void moveTask(db, me, task, status, rankAfter(column(status)));
  }
</script>

<section class="card" aria-label={`Board for ${show.name}`}>
  <div class="top">
    <div>
      <h2>{show.name}</h2>
      <p class="secondary">{show.venue} · {show.date} · doors {show.doors}</p>
    </div>
    <form onsubmit={add}>
      <label>New task <input bind:value={title} placeholder="Line check" /></label>
      <button class="primary" type="submit">Add task</button>
    </form>
  </div>
  <div class="board">
    {#each TASK_STATUSES as status (status)}
      <section class="column" aria-label={STATUS_LABELS[status]}>
        <h3>{STATUS_LABELS[status]}</h3>
        <ul>
          {#each column(status) as task (task.id)}
            <li class="task">
              <span>{task.title}</span>
              <select
                aria-label={`Status of ${task.title}`}
                value={task.status}
                onchange={(event) => move(task, event.currentTarget.value as TaskStatus)}
              >
                {#each TASK_STATUSES as option (option)}
                  <option value={option}>{STATUS_LABELS[option]}</option>
                {/each}
              </select>
            </li>
          {/each}
        </ul>
      </section>
    {/each}
  </div>
</section>
