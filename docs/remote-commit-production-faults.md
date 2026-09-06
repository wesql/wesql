# Remote Commit Production Fault Entrances

These controls are compiled into the ordinary release binary. They do not
require DBUG or WESQL_TEST. The default is disabled. No SQL user or remote
object can enable a control. An operator explicitly starts the process with
`WESQL_REMOTE_COMMIT_FAULT_DIR` pointing to a separate local control directory.
Use a fresh directory for each run and keep it outside every database root.

After SQL readiness and setup writes, create `<point>.arm` with exactly
`crash\n` or `pause\n`. Publish the arm atomically after writing its contents.
The next group at that boundary atomically claims the file, records and fsyncs
`<point>.hit` (point, process ID and action), then exits with code 86 or pauses
only the executing thread. `<point>.claimed` prevents a second hit. To resume
a pause, create `<point>.release`; alternatively kill the process externally.
An enabled control with invalid data or I/O errors stops the process.

| Case | Point or external entrance | Location |
| --- | --- | --- |
| 1 | remote_commit_crash_before_engine_wal_barrier | Before ha_flush_logs(true) |
| 2 | remote_commit_crash_after_engine_wal_before_binlog | After successful WAL flush, before writing transaction caches |
| 3 | remote_commit_crash_before_segment_put | After local binlog flush/sync and range validation, before sealing/upload |
| 4 | segment_response_lost | Proxy forwards actual immutable segment PUT, reads upstream success, drops downstream response; exact GET remains available |
| 5 | remote_commit_crash_after_segment_put_before_head_cas | After segment PUT/readback, before transition publication |
| 6 | head_response_lost | Proxy forwards actual HEAD CAS, reads upstream success, drops downstream response; exact GET remains available |
| 7 | remote_commit_pause_after_head_before_visibility | After successful HEAD publication, before status/public cursor changes |
| 8 | remote_commit_crash_after_head_cas_before_engine_commit | Before process_commit_stage_queue, after remote authorization |
| 9 | remote_commit_crash_after_engine_commit_before_reply | After engine commit and ACK_READY/cursor, before signal_done/client reply |
| 10 | remote_commit_delete_local_recovery | Harness receives actual SQL OK, kills source, records and empties all local database state, then starts recovery |
| 11 | remote_commit_writer_fencing | Pause before segment upload; take over on another root, then release the old writer |
| 12 | remote_commit_pause_after_read_head_before_cas | LOG transition only, after exact owner/HEAD check, before CAS with the saved ETag |
| 13 | head_response_unreconciled | Proxy applies HEAD CAS then loses its response and blocks subsequent remote requests from this writer |

Transport faults must be injected at real HTTP I/O. A fabricated return code,
sleep, or a fake object store does not exercise cases 4, 6 or 13. The proxy
records upstream applied results and all following GET/CAS observations.
Arm it only after fixture setup; use a separate direct connection for evidence
and the recovery writer. Case 4 also arms case 5 to stop after reconciliation
and leave HEAD unchanged. Case 6 also arms case 8 to exercise recovery of the
remote decision before engine commit.

The hit marker identifies a reached boundary, not a passing fault case.
Each case still needs client-result, exact HEAD/epoch/object, token/GTID and
empty-root recovery evidence according to the 13-case contract. Case 7 must
probe dump, status and the pending client while paused. Cases 11 and 12 must
retain both writers' records. Case 10 must retain before/after inventories;
only the fresh case's local roots may be deleted. Older failed evidence is
never an input to cleanup. ACK_READY is not proof that a client received OK.
