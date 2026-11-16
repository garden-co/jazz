// Documentation snippet sources for the docs todo app, compiled here and run by
// this module's tests. The docs include each `#region` by name; each region is
// one function, as in the other languages (docs/lib/languages/SNIPPETS.md).
package todoclient

import jazzapp.NewTodo
import jazzapp.Project
import jazzapp.Projects
import jazzapp.Todo
import jazzapp.Todos
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.launch
import tools.jazz.Jazz
import tools.jazz.JazzColumns
import tools.jazz.JazzException
import tools.jazz.JazzRow
import tools.jazz.Query
import tools.jazz.ReadTier
import tools.jazz.WriteTier
import tools.jazz.gather
import tools.jazz.include
import tools.jazz.select

// The example's owner and project, as the TypeScript snippets' example ids.
const val EXAMPLE_OWNER_ID = "00000000-0000-0000-0000-0000000000aa"
const val EXAMPLE_PROJECT_ID = "00000000-0000-0000-0000-000000000000"

// #region reading-oneshot-kotlin
suspend fun readOpenTodos(jazz: Jazz): List<Todo> =
    jazz.all(Todos.where(Todos.done eq false))
// #endregion reading-oneshot-kotlin

// #region reading-one-kotlin
// The first matching row, or null if no row matches or you can't read it.
suspend fun readTodo(jazz: Jazz, id: String): Todo? =
    jazz.one(Todos.where(Todos.id eq id))
// #endregion reading-one-kotlin

// #region reading-subscriptions-kotlin
// Collecting the flow subscribes, and each change brings the whole result.
// Cancel the returned job to unsubscribe.
fun subscribeOpenTodos(scope: CoroutineScope, jazz: Jazz, onChange: (List<Todo>) -> Unit): Job =
    scope.launch {
        jazz.watch(Todos.where(Todos.done eq false)).collect { todos -> onChange(todos) }
    }
// #endregion reading-subscriptions-kotlin

// #region reading-filters-kotlin
// Conditions are ANDed.
suspend fun readOpenDocsTodos(jazz: Jazz): List<Todo> =
    jazz.all(Todos.where(Todos.done eq false, Todos.title contains "docs"))
// #endregion reading-filters-kotlin

// #region reading-sorting-kotlin
suspend fun readOpenTodosByTitle(jazz: Jazz): List<Todo> =
    jazz.all(Todos.where(Todos.done eq false).orderBy(Todos.title.asc()))
// #endregion reading-sorting-kotlin

// #region reading-pagination-kotlin
suspend fun readTodoPage(jazz: Jazz, page: Int, pageSize: Int = 20): List<Todo> {
    val offset = maxOf(0, (page - 1) * pageSize)
    return jazz.all(
        Todos.where(Todos.done eq false)
            .orderBy(Todos.title.asc(), Todos.id.asc())
            .limit(pageSize)
            .offset(offset),
    )
}
// #endregion reading-pagination-kotlin

// #region reading-durability-tier-kotlin
// As the sync server has them, rather than what this client has locally.
suspend fun readOpenTodosAtRemoteTier(jazz: Jazz): List<Todo> =
    jazz.all(Todos.where(Todos.done eq false), tier = ReadTier.Remote)
// #endregion reading-durability-tier-kotlin

// #region reading-includes-kotlin
// Read an included row with `todo[Todos.project]`, or `todo.row(Todos.parent)`
// to go on to its own includes.
suspend fun readOpenTodosWithProjects(jazz: Jazz): List<JazzRow<Todo>> =
    jazz.all(
        Todos.where(Todos.done eq false)
            .include(Todos.project, Todos.parent.include(Todos.project)),
    )
// #endregion reading-includes-kotlin

// #region reading-reverse-relation-kotlin
// `projects.todos` is the reverse of `todos.project`: read it with `project[Projects.todos]`.
suspend fun readProjectsWithOpenTodos(jazz: Jazz): List<JazzRow<Project>> =
    jazz.all(Projects.include(Projects.todos.where(Todos.done eq false)))
// #endregion reading-reverse-relation-kotlin

// #region reading-recursive-kotlin
// From each open todo, its parent, the parent's parent and so on, up to 10 deep.
fun buildTodoLineageQuery(): Query<Todo> =
    Todos.where(Todos.done eq false).gather(Todos.parent, maxDepth = 10)
// #endregion reading-recursive-kotlin

// #region reading-select-kotlin
// Only the columns you read; `id` comes along always.
suspend fun readOpenTodoTitlesWithProjectNames(jazz: Jazz): List<JazzRow<Todo>> =
    jazz.all(
        Todos.where(Todos.done eq false)
            .select(Todos.title)
            .include(Todos.project.select(Projects.name)),
    )
// #endregion reading-select-kotlin

// #region reading-permission-advice-kotlin
// Each answer is true (allowed), false (denied), or null when no server can say.
suspend fun canReadTodo(jazz: Jazz, todoId: String): Boolean? =
    jazz.canRead(Todos, todoId)

suspend fun readTodosWithDeletePermission(jazz: Jazz): List<JazzRow<Todo>> {
    val todos = jazz.all(Todos.orderBy(Todos.title.asc()).select(Todos.title))
    return todos.filter { todo -> jazz.canDelete(Todos, todo.id) == true }
}

suspend fun readEditableTodos(jazz: Jazz): List<JazzRow<Todo>> {
    val todos = jazz.all(Todos.orderBy(Todos.title.asc()).select(Todos.title))
    return todos.filter { todo -> jazz.canUpdate(Todos, todo.id) { title(todo[Todos.title]) } == true }
}

suspend fun canCreateTodo(jazz: Jazz, title: String): Boolean? =
    jazz.canInsert(Todos, NewTodo(title = title, done = false))
// #endregion reading-permission-advice-kotlin

// #region writing-crud-kotlin
suspend fun writeTodoCrud(jazz: Jazz, todoId: String) {
    jazz.insert(
        Todos,
        NewTodo(title = "Write docs", done = false, owner_id = EXAMPLE_OWNER_ID, projectId = EXAMPLE_PROJECT_ID),
    )
    jazz.update(Todos, todoId) { done(true) }
    jazz.delete(Todos, todoId)
}
// #endregion writing-crud-kotlin

// #region writing-nullable-update-kotlin
suspend fun clearTodoOwner(jazz: Jazz, todoId: String) {
    // Setting a nullable column to null clears it; columns you don't set
    // (here `description`) are left unchanged.
    jazz.update(Todos, todoId) { owner_id(null) }
}
// #endregion writing-nullable-update-kotlin

// #region writing-upsert-kotlin
suspend fun upsertTodo(jazz: Jazz, importedTodoId: String) {
    jazz.upsert(Todos, importedTodoId, NewTodo(title = "Imported task", done = false))
    // Wait until the sync server has it.
    jazz.waitForSync(WriteTier.Global)
}
// #endregion writing-upsert-kotlin

// #region writing-restore-kotlin
suspend fun restoreDeletedTodo(jazz: Jazz, todoId: String): Todo {
    jazz.delete(Todos, todoId)

    checkNotNull(jazz.one(Todos.where(Todos.id eq todoId).includeDeleted())) { "Deleted todo not found" }

    // With a row as its new content; without one, it comes back as it was.
    return jazz.restore(
        Todos,
        todoId,
        NewTodo(title = "Restored task", done = false, owner_id = EXAMPLE_OWNER_ID, projectId = EXAMPLE_PROJECT_ID),
    )
}
// #endregion writing-restore-kotlin

// #region writing-durability-tier-kotlin
// Each call returns once the sync server accepted the write.
suspend fun writeTodoWithDurabilityTiers(jazz: Jazz) {
    val todo = jazz.insert(
        Todos,
        NewTodo(
            title = "Write docs with durability tier",
            done = false,
            owner_id = EXAMPLE_OWNER_ID,
            projectId = EXAMPLE_PROJECT_ID,
        ),
        tier = WriteTier.Global,
    )
    jazz.update(Todos, todo.id, tier = WriteTier.Global) { done(true) }
    jazz.delete(Todos, todo.id, tier = WriteTier.Global)
}
// #endregion writing-durability-tier-kotlin

// #region writing-mutation-errors-kotlin
// A write at WriteTier.Global returns once the server accepted it, and throws
// if the server rejects it. JazzException has no code field: its subtype says
// what kind of rejection it is (Permission when permissions deny it, Refused
// when it's invalid), and its message gives the reason.
suspend fun insertTodoAndWait(jazz: Jazz): Todo? =
    try {
        jazz.insert(
            Todos,
            NewTodo(title = "Ship review fixes", done = false, owner_id = EXAMPLE_OWNER_ID, projectId = EXAMPLE_PROJECT_ID),
            tier = WriteTier.Global,
        )
    } catch (e: JazzException.Permission) {
        System.err.println("permission denied: ${e.message}")
        null
    } catch (e: JazzException.Refused) {
        System.err.println("refused: ${e.message}")
        null
    }
// #endregion writing-mutation-errors-kotlin

// #region writing-transaction-kotlin
// The block's reads see its own writes. The transaction commits when the block
// returns (here: once the sync server accepted it) and rolls back if it throws.
suspend fun groupTodoWrites(jazz: Jazz, existingTodoId: String): String =
    jazz.transaction(tier = WriteTier.Global) { tx ->
        val created = tx.insert(
            Todos,
            NewTodo(title = "Write transaction docs", done = false, owner_id = EXAMPLE_OWNER_ID, projectId = EXAMPLE_PROJECT_ID),
        )

        tx.update(Todos, existingTodoId) { done(true) }

        val staged = tx.one(Todos.where(Todos.id eq created.id)) ?: error("Staged todo not found")
        staged.id
    }
// #endregion writing-transaction-kotlin

// #region writing-serializable-transaction-kotlin
// The server accepts it only if what it read hasn't changed concurrently;
// otherwise the commit throws JazzException.Conflict and nothing is applied.
suspend fun finishTodoSerializably(jazz: Jazz, todoId: String): String =
    jazz.transaction(tier = WriteTier.Global, serializable = true) { tx ->
        val todo = tx.one(Todos.where(Todos.id eq todoId)) ?: error("Todo not found")

        tx.update(Todos, todo.id) { done(true) }
        todo.id
    }
// #endregion writing-serializable-transaction-kotlin

// #region writing-transaction-errors-kotlin
// At WriteTier.Global the commit waits for the server; if the server rejects
// the transaction, nothing of it is applied and the commit throws.
// JazzException has no code field: its subtype (Permission or Refused) says
// what kind of rejection it is, and its message gives the reason.
suspend fun completeTodoInTransaction(jazz: Jazz, todoId: String) {
    try {
        jazz.transaction(tier = WriteTier.Global) { tx ->
            tx.update(Todos, todoId) { done(true) }
        }
    } catch (e: JazzException.Permission) {
        System.err.println("permission denied: ${e.message}")
    } catch (e: JazzException.Refused) {
        System.err.println("refused: ${e.message}")
    }
}
// #endregion writing-transaction-errors-kotlin

data class WhereOperatorExamples(
    val incompleteTodos: List<Todo>,
    val nonDraftTodos: List<Todo>,
    val selectedTodos: List<Todo>,
    val recentTodos: List<Todo>,
    val highPriority: List<Todo>,
    val lowPriority: List<Todo>,
    val matches: List<Todo>,
    val unlinkedTodos: List<Todo>,
    val linkedTodos: List<Todo>,
    val doneWithProject: List<Todo>,
    val recentIncomplete: List<Todo>,
)

suspend fun whereOperatorExamples(jazz: Jazz, searchTerm: String, todoIdA: String, todoIdB: String): WhereOperatorExamples {
    // #region where-eq-ne-kotlin
    // Exact match
    val incompleteTodos = jazz.all(Todos.where(Todos.done eq false))

    // Not equal
    val nonDraftTodos = jazz.all(Todos.where(Todos.title ne "Draft"))

    // One of a set
    val selectedTodos = jazz.all(Todos.where(Todos.id inList listOf(todoIdA, todoIdB)))
    // #endregion where-eq-ne-kotlin

    // #region where-numeric-kotlin
    val oneWeekAgo = System.currentTimeMillis() - 7 * 24 * 60 * 60 * 1000L

    val recentTodos = jazz.all(Todos.where(JazzColumns.createdAt gt oneWeekAgo))
    val highPriority = jazz.all(Todos.where(Todos.priority gte 3))
    val lowPriority = jazz.all(Todos.where(Todos.priority lt 10))
    // #endregion where-numeric-kotlin

    // #region where-contains-kotlin
    // Substring match (case-sensitive)
    val matches = jazz.all(Todos.where(Todos.title contains searchTerm))
    // #endregion where-contains-kotlin

    // #region where-null-kotlin
    // Rows where the optional ref is not set
    val unlinkedTodos = jazz.all(Todos.where(Todos.parentId.isNull()))

    // Rows where it is set
    val linkedTodos = jazz.all(Todos.where(Todos.parentId.isNotNull()))
    // #endregion where-null-kotlin

    // #region where-and-kotlin
    // done AND assigned to a project
    val doneWithProject = jazz.all(
        Todos.where(
            Todos.done eq true,
            Todos.projectId.isNotNull(),
        ),
    )
    // #endregion where-and-kotlin

    // #region where-order-limit-kotlin
    val recentIncomplete = jazz.all(
        Todos.where(Todos.done eq false).orderBy(JazzColumns.createdAt.asc()).limit(50),
    )
    // #endregion where-order-limit-kotlin

    return WhereOperatorExamples(
        incompleteTodos,
        nonDraftTodos,
        selectedTodos,
        recentTodos,
        highPriority,
        lowPriority,
        matches,
        unlinkedTodos,
        linkedTodos,
        doneWithProject,
        recentIncomplete,
    )
}

// #region where-subscription-kotlin
// Collecting the flow subscribes; each value is the whole result, updated
// whenever a todo enters or leaves the filter.
fun watchOpenTodos(jazz: Jazz): Flow<List<Todo>> =
    jazz.watch(Todos.where(Todos.done eq false))
// #endregion where-subscription-kotlin
