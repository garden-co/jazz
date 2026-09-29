# EpicDrop learnings

The app uses only public APIs. `File.stream()` feeds typed `Db.insertStreaming` through a small
async generator that reports progress and fails the stream on cancel; a failed stream publishes no
row. The folder query is scoped through indexed `folder_id` and selects only metadata. The browser
receipt asserts that the listing row has no `contents` property, so removing the projection fails
it.

Previews use the partial large-value selection from #2088 (`select({ contents: { from, to } })`).
Text previews decode each 64 KB page with a streaming `TextDecoder`, so a code point split across
two pages stays intact. Whole-value reads are used only for download and for media, which the
browser needs as one Blob to seek.

Sharing follows the invite-link recipe without a backend route: the membership insert itself
carries the invite code, and its permission checks a matching `folderInvites` row at the sync
server, which can see invites the caller cannot read. The role is part of the match, so a viewer
link cannot be replayed as an editor membership. The membership row keeps its code, which makes
it as sensitive as the invite: only the member and the folder owner may read it, otherwise a viewer
could lift an editor's code from a co-member's row. Inherited folder access uses bounded recursive
`allowedTo` (`maxDepth: 8`).

The native benchmark is a correctness companion to the UI rather than a second application model:
it creates one bounded-reader file, lists the same metadata shape, then reads one bounded range
through the Rust Db API.

Open product work: controlled remote withholding (#1862), persistent-worker large-value relay
(#1978), exact chunk demand for range reads (#2090), and the native mounted folder.
