/**
 * Every CodSpeed name that main measured on every merge before the benchmarks
 * were consolidated into the hero examples (#3730), and what became of it:
 *
 * - `renamed`: a per-merge case now measures the same workload under
 *   `successor`. `stitch` is true only when same-base CodSpeed runs showed the
 *   two equivalent, so the docs timeline may continue the former name's
 *   history under the new one (hero cards keep their release history). Moves
 *   whose numbers changed (the W1 suite, which moved to mimalloc: −7% to
 *   −55%) restart their history instead.
 * - `nightly`: the same name, now measured by the nightly CodSpeed run on main
 *   (a `nightly` bench target, or Groove's `GROOVE_BENCH_SWEEP` sweep), not on
 *   every merge. Its history continues under that name.
 * - `dropped`: no longer measured. `coveredBy` names the case that measures the
 *   closest operation, when there is one.
 *
 * Results recorded under a former name, such as reviewed release backfills,
 * remain valid history for that name.
 */
export type FormerBenchmark =
  | { kind: "renamed"; successor: string; stitch: boolean }
  | { kind: "nightly" }
  | { kind: "dropped"; coveredBy?: string };

const renamed = (successor: string, stitch = false): FormerBenchmark => ({
  kind: "renamed",
  successor,
  stitch,
});
const nightly: FormerBenchmark = { kind: "nightly" };
const dropped = (coveredBy?: string): FormerBenchmark =>
  coveredBy ? { kind: "dropped", coveredBy } : { kind: "dropped" };
const sized = (names: string[], sizes: (number | string)[], fate: FormerBenchmark) =>
  names.flatMap((name) => sizes.map((size) => [`${name}[${size}]`, fate] as const));

export const formerBenchmarks: ReadonlyMap<string, FormerBenchmark> = new Map([
  // Todo example's native workload → StagePlan's task list (same code, same
  // allocator: equivalent on same-base runs).
  ["sequential_insert_1350_rocksdb", renamed("stage_plan_add_task_1350", true)],
  ["sequential_update_1350_rocksdb", renamed("stage_plan_check_off_task_1350", true)],
  ["batch_update_1350_rocksdb", renamed("stage_plan_bulk_complete_1350", true)],
  ["reopen_1500_rocksdb", renamed("stage_plan_reopen_1500", true)],

  // W1 team task board → StagePlan's show board. Now built with mimalloc, so
  // the history restarts.
  ["query_board_profile_s_rocksdb", renamed("stage_plan_open_board")],
  ["query_task_detail_profile_s_rocksdb", renamed("stage_plan_open_task_detail")],
  [
    "query_bounded_activity_page_scaling_rocksdb[30000]",
    renamed("stage_plan_activity_page[30000]"),
  ],
  ["subscribe_activity_intersection_delta_rocksdb", renamed("stage_plan_move_card_to_done")],
  ["subscription_fanout_memory[(600, 60)]", renamed("stage_plan_crew_dashboard[(600, 60)]")],
  ["subscription_fanout_memory[(6000, 60)]", renamed("stage_plan_crew_dashboard[(6000, 60)]")],
  ["update_activity_policy_scaling_memory[9000]", renamed("stage_plan_update_under_policy[9000]")],
  [
    "subscribe_activity_policy_point_scaling_memory[9000]",
    renamed("stage_plan_subscribe_under_policy[9000]"),
  ],
  ["delete_task_point_scaling_memory[9000]", renamed("stage_plan_archive_task[9000]")],
  ["restore_task_point_scaling_memory[9000]", renamed("stage_plan_restore_task[9000]")],
  [
    "resume_one_task_update_scaling_memory[(500, 2000, 1500)]",
    renamed("stage_plan_resume_after_offline[(500, 2000, 1500)]"),
  ],
  // W1 scaling points and diagnostics in StagePlan's `nightly` target.
  ["query_bounded_activity_page_profile_s_rocksdb", nightly],
  ["query_bounded_activity_page_scaling_rocksdb[9000]", nightly],
  ...sized(["query_bounded_activity_page_scaling_memory"], [9000, 30000], nightly),
  ...sized(
    ["query_comments_scaling_rocksdb", "query_comments_scaling_memory"],
    ["(300, 1200, 900)", "(3000, 12000, 9000)"],
    nightly,
  ),
  ["update_activity_indexed_predicate_no_subscription_rocksdb", nightly],
  ["update_activity_indexed_predicate_no_subscription_memory", nightly],
  ...sized(["subscribe_activity_point_scaling_memory"], [900, 9000], nightly),
  ["subscribe_activity_policy_point_scaling_memory[900]", nightly],
  ["resubscribe_activity_point_profile_s_memory", nightly],
  // W1 in-memory twins and smaller points: a per-merge case measures the same
  // operation.
  ["query_board_profile_s_memory", dropped("stage_plan_open_board")],
  ["query_task_detail_profile_s_memory", dropped("stage_plan_open_task_detail")],
  ["query_bounded_activity_page_profile_s_memory", dropped("stage_plan_activity_page[30000]")],
  ["subscribe_activity_intersection_delta_memory", dropped("stage_plan_move_card_to_done")],
  [
    "update_activity_policy_no_subscription_memory",
    dropped("stage_plan_update_under_policy[9000]"),
  ],
  ["update_activity_policy_scaling_memory[900]", dropped("stage_plan_update_under_policy[9000]")],
  ["delete_task_point_scaling_memory[900]", dropped("stage_plan_archive_task[9000]")],
  ["restore_task_point_scaling_memory[900]", dropped("stage_plan_restore_task[9000]")],
  [
    "resume_one_task_update_scaling_memory[(300, 1200, 900)]",
    dropped("stage_plan_resume_after_offline[(500, 2000, 1500)]"),
  ],
  ["subscription_fanout_memory[(600, 0)]", dropped("stage_plan_crew_dashboard[(600, 60)]")],
  ["subscription_fanout_memory[(600, 10)]", dropped("stage_plan_crew_dashboard[(600, 60)]")],
  // W1 ahead-current history → Wequencer's pad edit history.
  ["w1_local_ahead_current_history[100]", dropped("wequencer_pad_edit_history[1000]")],
  ["w1_local_ahead_current_history[1000]", renamed("wequencer_pad_edit_history[1000]")],
  ["w1_local_ahead_current_history[10000]", renamed("wequencer_pad_edit_history[10000]")],

  // Chat example → BandChat's members-only room (equivalent on same-base runs).
  ["chat_open_chat[10000]", renamed("band_chat_open_room[10000]", true)],
  ["chat_send_100[10000]", renamed("band_chat_send_100[10000]", true)],
  ["chat_open_chat[1000]", dropped("band_chat_open_room[10000]")],
  ["chat_send_100[1000]", dropped("band_chat_send_100[10000]")],
  // Auth chat example → BandChat's claim-gated announcements room: one case
  // keeps claim-gated policies and a live full-history view under writes.
  ...sized(
    ["auth_chat_open_room", "auth_chat_send"],
    [1000, 10000],
    dropped("band_chat_post_announcements[10000]"),
  ),
  // BandChat points no longer measured.
  ...sized(["band_chat_author_history"], [1024, 4096], dropped("stage_plan_activity_page[30000]")),
  ["band_chat_timeline_second_page[1024]", dropped("band_chat_timeline_second_page[4096]")],
  ["band_chat_unread_recent_rooms[1024]", dropped("band_chat_unread_recent_rooms[4096]")],
  ["band_chat_caught_up_fast_resume[1000]", dropped("band_chat_caught_up_fast_resume[10000]")],
  // crates/jazz route subscription curve and selective hydration → the apps.
  ["matching_write_fanout[100]", renamed("band_chat_new_message_rooms_open[100]", true)],
  ["attach_route_bindings[100]", renamed("wequencer_open_pattern_views[100]")],
  ["maintained_subscription_hydration_100k", renamed("big_label_releases_live_view_100k", true)],
  ["maintained_subscription_hydration_10k", dropped("big_label_releases_live_view_100k")],

  // Policy-scoped documents → BandBook's workspace pages. Renamed at 100k;
  // every other arm and the 10k table run nightly under their former names.
  ["policy_free_org_page50[100000]", renamed("band_book_workspace_pages_unrestricted[100000]")],
  ["owner_or_org_policy_org_page50[100000]", renamed("band_book_workspace_pages[100000]")],
  ["owner_policy_page50[100000]", renamed("band_book_my_pages[100000]")],
  [
    "subscribe_owner_or_org_policy_org_page50[100000]",
    renamed("band_book_workspace_pages_live[100000]"),
  ],
  ...sized(
    [
      "policy_free_owner_page50",
      "owner_policy_page50",
      "owner_or_org_policy_owner_page50",
      "policy_free_org_page50",
      "owner_or_org_policy_org_page50",
      "subscribe_policy_free_owner_page50",
      "subscribe_owner_policy_page50",
      "subscribe_owner_or_org_policy_owner_page50",
      "subscribe_policy_free_org_page50",
      "subscribe_owner_or_org_policy_org_page50",
    ],
    [10000],
    nightly,
  ),
  ...sized(
    [
      "policy_free_owner_page50",
      "owner_or_org_policy_owner_page50",
      "subscribe_policy_free_owner_page50",
      "subscribe_owner_policy_page50",
      "subscribe_owner_or_org_policy_owner_page50",
      "subscribe_policy_free_org_page50",
    ],
    [100000],
    nightly,
  ),

  // Files: one large-value range read (equivalent on same-base runs).
  ["epic_drop_seek_64mb", renamed("record_player_scrub_track_64mb", true)],
  ["music_agent_attachment_seek_8mb", dropped("record_player_scrub_track_64mb")],
  ["epic_drop_upload_4mb", dropped("epic_drop_upload_64mb")],

  // Smaller points of cases that stay.
  ["poster_shop_open_canvas[512]", dropped("poster_shop_open_canvas[4096]")],
  ["world_tour_member_calendar_window[128]", dropped("world_tour_member_calendar_window[4096]")],
  ["world_tour_public_calendar_window[128]", dropped("world_tour_public_calendar_window[4096]")],
  ...sized(
    ["big_label_artist_load", "big_label_catalog_load"],
    [512, 4096],
    dropped("big_label_label_load[4096]"),
  ),
  ["big_label_label_load[512]", dropped("big_label_label_load[4096]")],
  ["big_label_ingest_batch_amortization[10]", dropped("big_label_ingest_batch_amortization[100]")],
  ["ingest_walltime_10k", dropped("ingest_walltime_100k")],

  // Groove: the per-merge Engine rows keep only the IVM engines at the larger
  // size; the rest of the sweep runs nightly (GROOVE_BENCH_SWEEP=1). Each name
  // repeats once per scenario module.
  ...sized(["prepared_warm", "prepared_cold", "pull", "snapshot"], [500], nightly),
  ...sized(["pull", "snapshot"], [5000], nightly),
  ...sized(
    ["ivm", "pull_touched", "snapshot_touched", "sqlite_all", "sqlite_touched"],
    [10],
    nightly,
  ),
  ...sized(["pull_touched", "sqlite_all", "sqlite_touched"], [100], nightly),
]);

/** Former name → the per-merge case that measures the same workload, or null. */
export const formerBenchmarkNames: ReadonlyMap<string, string | null> = new Map(
  [...formerBenchmarks].map(([name, fate]) => [
    name,
    fate.kind === "renamed" ? fate.successor : null,
  ]),
);

/** Current name → the former name whose history it continues. */
export const stitchedFormerNames: ReadonlyMap<string, string> = new Map(
  [...formerBenchmarks].flatMap(([name, fate]) =>
    fate.kind === "renamed" && fate.stitch ? [[fate.successor, name] as const] : [],
  ),
);
