# EpicDrop foundation receipts

The app uses only public APIs: `File.stream()` feeds typed `Db.insertStreaming`, and its query is
scoped through indexed `folder_id` while selecting only metadata. The browser receipt intentionally
asserts that the resulting row has no `contents` property; removing the projection makes that
receipt fail.

The native benchmark is a correctness companion to the UI rather than a second application model:
it creates one bounded-reader file, lists the same metadata shape, then reads one bounded range
through the Rust Db API. Its source-reader receipt protects the benchmark's 32 KiB input-window
claim.

Future product work remains explicit: typed browser page selection/editing (#1833), controlled
remote withholding (#1862), and persistent-worker large-value relay correctness (#1978). Do not
work around those contracts through private runtime helpers.
