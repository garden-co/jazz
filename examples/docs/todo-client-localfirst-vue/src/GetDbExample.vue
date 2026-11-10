<!-- #region writing-get-db-vue -->
<script setup lang="ts">
import { useDb } from "jazz-tools/vue";

const db = useDb();

// Query composables return the same handle, so a component that reads and writes needs one:
// const { data: todos, db } = useAll(app.todos);
</script>
<!-- #endregion writing-get-db-vue -->
