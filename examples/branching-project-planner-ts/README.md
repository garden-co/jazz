# Branching project planner

This browser example models planning scenarios as ordinary `scenarios` rows. `tasks.scenario_id`
is both a normal reference and the table's `branchBy` column. The draft view overlays a live draft
scenario over main, then demonstrates copy-on-write by editing an inherited task.

```sh
pnpm --filter branching-project-planner-ts dev
```

The example deliberately keeps branch lifecycle (`status`), base resolution
(`base_scenario_id`), and UI in application code. Jazz only qualifies task history and reduces the
head over its supplied base. Its task write policy also traverses the ordinary `scenario_id`
reference and permits changes only while that application-owned scenario is open.
