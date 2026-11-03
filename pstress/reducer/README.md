# pstress_reducer

Reduces a captured failing pstress workload into a smaller reproducer of the **same target
failure** while keeping independent database sessions. Several sessions can be necessary at
the same time; statements are still reduced inside each surviving session. The result is a
directory (bundle) holding the setup state, one SQL stream per concurrent session, the
server configuration and replay instructions. It is not necessarily a single SQL file.

It is a new tool. It borrows ideas from `reducer.sh` in Percona-QA/percona-qa (verify first,
fresh state per trial, adaptive chunk removal, single-statement passes, saving candidates
with replay artifacts) but does not run it or port it. Unlike the ThreadSync path there, it
keeps reducing when more than one session survives.

## Status and scope (first release)

* Linux, one standalone MySQL / Percona Server instance per attempt, one attempt at a time.
  Concurrent SQL sessions run inside each attempt.
* Input: pstress thread logs for **one explicitly selected step**, plain SQL files, or a
  normalized manifest you write yourself.
* Not supported yet: multi-node (PXC / group replication) incidents (the importer rejects
  them), timing/schedule search, SQL simplification, setup/data reduction, startup-option
  reduction, parallel attempts, and crash-recovery incidents. See "Later milestones".

What it does **not** claim: deterministic race reproduction, a globally minimal SQL set,
complete SQL dependency analysis, or faithful reproduction of physical recovery from a logical
dump.

## Install

```
python3 --version                       # 3.11+ required (3.10 runs the importer and unit tests)
python3 -m pip install -r pstress/reducer/requirements.txt   # PyMySQL==1.1.1
```

Only the standard library and PyMySQL are used. The `mysql` command-line client is needed
when setup SQL is loaded with the default `mysql-cli` loader.

Versions tested for this release:

| Component | Version |
|---|---|
| Python | 3.13.16 (integration and unit tests), 3.10.12 (unit tests) |
| PyMySQL | 1.1.1 |
| Server | MySQL 8.0.46 (Ubuntu 24.04 package), `caching_sha2_password` root with empty password over a Unix socket |
| pstress (log format) | cf59749 (`PSTRESS-173`), built with `-DMYSQL=ON` |
| Reference reducer | percona-qa a6a7debf (read for ideas only) |

Check PyMySQL against your own server and authentication plugin before you rely on it. The
quickest check is `verify` with a small workload.

## Workflow

```
R=pstress/reducer/pstress_reducer.py

# 1. import: raw logs + explicit step + explicit initial state -> normalized manifest
$R import --incident-dir /data/run/7 --step 7 \
          --setup snapshot:/data/run/6/data --out /tmp/inc7

# 2. verify: is the target reproducible from the import at all?
$R verify --workload /tmp/inc7 --config my-config.json --workdir /tmp/inc7-verify

# 3. reduce: baseline, then session/chunk/statement deletion, final validation, bundle
$R reduce --workload /tmp/inc7 --config my-config.json --workdir /tmp/inc7-work

# Ctrl+C at any time; then
$R resume --workdir /tmp/inc7-work
$R status --workdir /tmp/inc7-work

# 4. replay the exported bundle on its own (it does not need the incident directory)
$R replay --workload /tmp/inc7-work/bundle --config /tmp/inc7-work/bundle/config.json \
          --workdir /tmp/replay --attempts 10
```

`import` needs no server. All other commands start their own `mysqld` instances. Each
instance gets a datadir, socket, pid file, tmpdir and error log under the work directory.

## Commands

| Command | What it does |
|---|---|
| `import` | Parses `<node>_step_<N>_thread-<K>.sql` logs (or `--sql-file`, or `--manifest`) into `manifest.json`. It also writes `import-report.{json,txt}` and readable `sessions/*.sql`. |
| `verify` | Runs N independent attempts from restored state, with no reduction. Reports target hits, valid misses, invalid runs and unresolved runs separately. |
| `replay` | Runs one or more attempts of a workload or bundle and keeps all attempt directories. |
| `reduce` | Runs the baseline, the empty-workload probe, hierarchical deletion and final validation, then exports the bundle. |
| `resume` | Continues a saved reduction. It refuses to continue if the environment, setup, oracle or replay policy changed, unless `--revalidate` is given. `--extend` continues a finished run, for example with a larger budget. |
| `export` | Writes the best candidate, or `--candidate <hash>`, as a bundle. |
| `status` | Prints the saved state. |

### Exit codes

| Code | Meaning |
|---|---|
| 0 | Verified result, or successful command. |
| 1 | Internal error. |
| 2 | Usage or configuration error. |
| 3 | Baseline unconfirmed: the target was not observed within the attempt budget. |
| 4 | Repeated infrastructure or invalid-run failures, such as startup, setup or connect failures. |
| 5 | Budget exhausted. A partial, confirmed best candidate was saved and validated. |
| 6 | Final validation failed. Either an earlier confirmed candidate was used instead, or the bundle is marked unvalidated. |
| 7 | The importer found records it cannot frame. Fix the input, or pass `--skip-unframed`. |
| 130 | Interrupted. The best confirmed candidate is kept, and `resume` continues from there. |

## Import

The log format is derived from `execute_sql()` in `src/random_test.cpp`:

```
[<ts> <start>=><dur>ms ]* S <sql> rows:<n>
[<ts> <start>=><dur>ms ]* F <sql>
Error <mysql_error()>
server gone, while processing <sql>        (CR_SERVER_GONE / CR_SERVER_LOST only)
```

* **Order and failures.** Successful and failed statements are both kept by default, in
  original order, including duplicates. Failed SQL can matter for locking and for the bug
  trigger. A `server gone` line is de-duplicated against its `F` record. If there is no `F`
  record, the line is imported as a `LOST` statement and labelled as such.
* **Parsing.** Only the anchored prefix and one trailing ` rows:<n>` are parsed. ` S `, ` F `
  and `rows:` inside SQL text are never touched. Multi-line SQL and multi-line error text
  are framed, and the importer warns when it sees them.
* **Records it cannot use.** An incomplete final record, for example one cut off by a
  crash, is reported and not imported unless you pass `--include-incomplete`. A record that
  cannot be framed stops the import with exit code 7, unless you pass `--skip-unframed`.
  Unknown lines are listed in the report. Raw files are never modified.
* **Timing.** Timing prefixes are kept as raw metadata. At cf59749 both numbers are
  microseconds even though the suffix says `ms`. The unit is recorded from the pstress
  revision, which is detected from the ddl log, and is never inferred from the suffix. For a
  revision the importer doesn't know, pass `--duration-unit us` (or `ms`) explicitly.
* **Step and node selection.** You must choose the step explicitly when there are several
  (`--step`), and successive steps are never combined. More than one node is rejected,
  because multi-node replay is unsupported. `--node` imports a single node's sessions only,
  with a note that the result is not a faithful multi-node reproduction.
* **Setup is required.** Choose one of:
  * `--setup none`: start from a fresh initialized datadir. This is right for step 1 of a
    pstress-run.
  * `--setup sql:<dump>`: a logical dump, loaded with the `mysql` client on every attempt,
    so `DELIMITER` works.
  * `--setup snapshot:<datadir>`: a consistent offline copy, made by a clean shutdown or a
    prepared backup. It is checksummed, copied (never hard-linked) for every attempt, and
    never modified.

  **Encryption keyrings.** pstress-run keeps the keyring file inside the datadir and writes
  `<datadir>/component_keyring_file.cnf` with an absolute path into the run directory. That
  path no longer exists after the trial is moved. Each attempt's copy of the snapshot gets
  the `path` repointed to its own copy of the keyring (`server.relocate_keyring`, on by
  default; see `trials/<n>/prepare-notes.txt`), so encrypted tablespaces open as they did in
  the original run. The snapshot itself is not changed. The import report lists the keyring
  configs it found, and says so if a keyring lies outside the snapshot. The global
  `mysqld.my` and `lib/plugin/component_keyring_file.cnf` that pstress-run creates in the
  basedir must still be present.

  Steps above 1 need the state left by the previous step. The importer refuses an empty
  setup for them unless you pass `--allow-empty-setup`, and it points at a saved
  `<N-1>/data` if one exists. Use `--external-file` to record files a snapshot depends on,
  such as keyring files.
* **Session settings.** Session ids are `<node>:step<N>:thread-<K>`. Statement ids are
  `t<K>.<seq>`. The session database comes from a leading `USE`, otherwise from `--database`
  (default `test`). Autocommit stays at the server default, as libmysqlclient does in
  pstress, unless you pass `--autocommit on|off`. PyMySQL's own default is never used.
* **Plain SQL.** `--sql-file` (converter output) is one statement per line, and each file
  is one session. It is labelled as missing capture metadata.

### Manifest (format version 1)

`manifest.json` is the source of truth. It records exact statement boundaries, so replay
never splits on semicolons. The `sessions/*.sql` files are only for reading. The docstring
of `reducerlib/manifest.py` documents the full schema. A minimal hand-written example:

```json
{"format": "pstress-reducer-workload", "format_version": 1,
 "setup": {"kind": "sql", "path": "setup.sql", "loader": "mysql-cli", "create_databases": ["test"]},
 "eof_policy": "hold-until-all-finished",
 "sessions": [
   {"id": "A", "database": "test", "autocommit": "server-default",
    "statements": [{"id": "A1", "sql": "BEGIN"}, {"id": "A2", "sql": "UPDATE t SET v=1 WHERE id=1"},
                   {"id": "A3", "sql": "COMMIT", "pinned": true}]},
   {"id": "B", "database": "test",
    "statements": [{"id": "B1", "sql": "UPDATE t SET v=2 WHERE id=1"}]}],
 "groups": [["A2", "B1"]]}
```

Relative setup paths resolve against the manifest's directory.

* `pinned: true` on a statement keeps it.
* `pinned: true` on a session keeps all of that session's statements verbatim.
* `groups` lists statements that can only be removed together. A group can span sessions.

## Replay model

Each attempt runs this sequence: prepare a fresh copy of the template or snapshot, start
`mysqld` (`--no-defaults`, Unix socket, `--skip-networking`, `--mysqlx=OFF`, plus your
`extra_args`), wait for readiness, run the optional `post_start` hook, load the setup, start
one worker process per session, replay, collect and classify evidence, then clean up.

* Each worker opens its own connection and runs its stream in order. It consumes every
  result set before sending the next statement. It never reconnects, never wraps statements
  in a transaction and never commits implicitly.
* All workers connect first. A common start gate then releases them together. Statements
  are **not** gated against each other, so a session blocked on a lock never stops another
  session from committing.
* Ordinary server SQL errors are recorded and the session continues. Client and transport
  errors (CR_* 2000–2999) end that session. `trial.sql_error_policy` can turn chosen error
  codes into a session abort or an INVALID attempt.
* When a session reaches end of stream, its connection stays open until every session has
  finished or the attempt ends. Then all connections close, so an open transaction is
  rolled back by the server. This policy is recorded in each bundle.
  Side effect: a candidate that drops a `COMMIT` can leave locks held until the other
  sessions finish, so those sessions wait up to `innodb_lock_wait_timeout` (50s by
  default). Metadata-lock waits for DDL are worse: `lock_wait_timeout` defaults to a year,
  so they run until `replay_timeout` and the attempt is UNRESOLVED. The candidate is still
  judged correctly, but attempts get slow. If the bug does not depend on these settings,
  consider `"extra_args": ["--innodb-lock-wait-timeout=5", "--lock-wait-timeout=10"]` and
  a `replay_timeout` close to the normal replay time. Alternatively, set
  `"eof_policy": "close-when-finished"` in the manifest so that finished sessions
  disconnect at once.
* Each worker logs every statement before sending it and after it completes, with
  `CLOCK_MONOTONIC` timestamps (`trials/<n>/replay/s*.events.jsonl`). This records the
  actual replay order. It does not reconstruct the original interleaving.
* Timeouts: connect and per-query socket timeouts inside each worker, a replay deadline, and
  bounded hooks, setup load and checker. When a deadline expires, the reducer kills the
  process groups it started, and only those. It never uses `killall` or name matching, and
  it never calls `replay_test.sh`.
* **Retention.** Datadirs are deleted after every attempt by default. The last N target
  attempts and N other attempts keep their logs.

## Failure oracle

Each attempt ends as `TARGET`, `NO_TARGET`, `INVALID` or `UNRESOLVED`. Unrelated crashes are
counted separately as "other failures". Only this attempt's error log is read, and it is cut
into phases: startup, setup, replay, and shutdown.

| `oracle.kind` | TARGET when |
|---|---|
| `crash` | The owned `mysqld` terminated without the reducer asking it to, in the replay phase (or in shutdown, with `phase: replay+shutdown`), **and** every `required` regex matches that phase's log **and** no `excluded` regex matches. |
| `pattern` | Every `required` regex matches the replay-phase error log and/or the session error list (`sources`: `error_log`, `session_errors`). |
| `sql_probe` | After replay, `probe.query` runs on a fresh connection and its tab-separated result matches `probe.expect_regex`. |
| `checker` | `checker.argv` exits with a code listed in `target_codes`. `no_target_codes` and `invalid_codes` must also be mapped explicitly; an unmapped exit code is INVALID. The checker receives `PRD_EVIDENCE` and `PRD_ATTEMPT_DIR`, and the server is still running when it is called. |

Some outcomes never count as the target:

* A kill sent by the reducer.
* A signature found in the startup or setup phase. A startup or setup failure is `INVALID`,
  and if the target signature appears there the report says so.
* A disconnect or timeout on its own.
* Text from a previous attempt.

### Acceptance policy (defaults; all configurable)

These counts are heuristic reproducibility checks, not statistical proofs.

* **Baseline.** 10 independent attempts of the unreduced workload. With zero hits, the run
  stops (exit code 3).
* **Empty-workload probe.** Two attempts with no session SQL. If the target appears, the
  report says that the setup or startup alone reproduces it.
* **Candidate acceptance.** A candidate is accepted after 2 hits, within at most 10 valid
  attempts. It is rejected early once 2 hits are no longer reachable. A rejection is
  remembered only for the current round, so a flaky negative is never cached permanently.
* **Final validation.** At least 5 hits within 30 valid attempts. If this fails, the
  reducer tries the previous confirmed candidates (`final_fallbacks`) and reports which one
  passed.
* **Global caps.** `max_total_attempts`, `max_seconds` and `max_consecutive_infra_failures`.
* **Hit-rate warning.** If an accepted candidate's hit rate falls below a quarter of the
  baseline rate (`low_hit_rate_ratio`), the reducer logs a warning.

A miss within budget means the target was not confirmed, not that it is impossible. For
scale: if a bug has a true 10% hit rate, 10 attempts miss it entirely about 35% of the time.

Evidence is cached under a key that covers the workload, the setup checksum, the server
binary hash and version, the startup options, the template, the hooks, the oracle and the
replay policy. Evidence is never reused after any of these change.

## Reduction

Each round runs these passes. Every pass is judged by the same replay, oracle and
acceptance policy.

1. **tail-trim.** Remove statements that were never sent in the latest target attempt.
   This is a cheap first guess and still goes through the oracle.
2. **sessions.** ddmin over whole sessions.
3. **aligned-chunks.** Remove the same relative slice from every session at once, so that
   paired fragments in different sessions can disappear together.
4. **transactions.** Remove recognized `BEGIN` / `START TRANSACTION` … `COMMIT` /
   `ROLLBACK` blocks. This is a heuristic. Implicit commits and `autocommit=0` are not
   modelled, and later passes still reduce inside blocks.
5. **statements.** Per-session ddmin (complement search) down to single statements.

Rounds repeat until a full round accepts nothing or the budget runs out. Only strictly
smaller candidates are tested. The cost is lexicographic: (active sessions, retained
non-pinned statements, SQL bytes). Pinned statements and the setup are reported separately.
There is no dependency solver: essential DDL survives because the oracle needs it.

When the last round accepted nothing and every observed attempt reproduced, the report calls
the result *statement-level 1-minimal under this policy*. Otherwise it says that no further
reduction was confirmed within budget.

## Work directory and checkpoints

```
workload.json  config.effective.json  state.json  cache.json  journal.jsonl  reducer.log
candidates/<sha256>.json   evidence/<hash>/attempt-*.json   trials/<n>/   template/   bundle/
report.json  report.txt   LOCK
```

* Candidates are stored by content hash.
* `state.json` is replaced atomically. It holds the best confirmed candidate, the history,
  the pass cursor, attempts and time used, the baseline and the final-validation counts.
* `journal.jsonl` is an append-only record of every attempt and every decision.
* No random proposals are used yet, so no random number generator state is stored.

## Bundle

`bundle/` holds:

* `manifest.json` and `sessions/*.sql`.
* `setup/`: a copied SQL dump, or a reference to the snapshot with its checksum. Use
  `--include-snapshot` to copy the snapshot itself.
* `config.json`, `environment.json` (server version, binary hash, startup options),
  `replay-policy.json` and `oracle.json`.
* `validation.json`, `history.json` and `evidence/`.
* `README.md`, which states whether the bundle is portable or still needs an external
  snapshot or server binary.

Multi-session bundles keep their concurrency instructions. They are never flattened into
one SQL file.

## Tests

```
cd pstress/reducer
python3 -m unittest discover -s tests -v                              # importer + reduction logic (no server)
PRD_MYSQLD=/path/to/mysqld python3 -m unittest tests.test_integration # replay tests on real servers
PRD_SLOW=1 PRD_MYSQLD=/path/to/mysqld python3 -m unittest tests.test_integration.EndToEndReduction
```

What each part covers:

* **Unit tests.** S/F records, duplicates, error and diagnostic lines, metadata-like text in
  literals, multi-line statements and errors, quotes, comments and semicolons, incomplete and
  unframed records, the duration-unit quirk, ambiguous step and node selection, non-UTF-8
  bytes, and real cf59749 log fixtures. The reduction tests use injected deterministic or
  flaky oracles. They cover two required sessions, whole-session removal, pinning, groups,
  non-monotonic dependencies, paired cross-session fragments, transaction blocks, tail-trim,
  negative caching per round, cache-key invalidation, interrupt and resume, budget
  exhaustion, and fallback after failed final validation.
* **Integration tests.** These run on a real `mysqld`.
  * A lock overlap: one session commits while another is blocked, and the replay is
    neither serialized nor deadlocked.
  * Temporary tables, prepared statements, user variables and `autocommit=0` all persist
    within a session, and no implicit commit happens at end of stream.
  * A setup that uses `DELIMITER`, and a `CALL` that returns several result sets.
  * A snapshot setup that is copied for each attempt and stays unmodified.
  * Fresh state on every attempt.
  * Blocked queries and slow hooks are terminated within their limits, and an unrelated
    process named `mysqld` is left alone.
  * Startup errors, reducer-issued kills, disconnects, unrelated crashes and stale logs never
    count as the target.
  * Server error codes of 3000 and above do not end a session.
  * End to end: a crash injected by `tests/helpers/crashing_mysqld.py` needs one statement
    from each of two sessions. The test reduces it, interrupts with SIGINT, resumes, and
    replays the exported bundle on its own.

## Representative run on real pstress logs

No real failing incident was available while this was built, so the run below does not
reproduce a server bug. Its target is a genuine concurrency effect of a real pstress
workload: an InnoDB deadlock (`ER_LOCK_DEADLOCK`, error 1213).

**Workload.** pstress cf59749 ran against MySQL 8.0.46 with 3 threads for about 3 seconds
(`--log-all-queries --log-query-duration`, step 1). The import gave 3 sessions and 3,721
statements (3,379 S, 342 F).

**Oracle.** `pattern` on `session_errors`, `" 1213 Deadlock found"`.

**Baseline.** 3 of 30 attempts hit the target, a hit rate of 0.10. About 400 of thread 0's
first statements failed with error 1146 ("table doesn't exist") during replay. In the
original run, thread 0 waited for thread 1's initial load, and that cross-session ordering
is not in the logs.

**Reduction.** The run used `candidate_accept_hits=1` and `candidate_max_valid=6`. It was
killed once with SIGKILL and resumed from `state.json` without problems. After 2,700 seconds
the time budget ran out, at a cost of (3, 3721, 209740) → (2, 1826, 104002). The deadlock
needs two sessions, and both survived.

**Final validation.** The policy required 5 hits in 20 attempts. All three tried candidates
failed, at 0/16, 0/16 and 1/17. The tool therefore labelled the bundle
`final-validation-failed`.

This is the intended honest outcome when the hit rate is about 10%. Accepting a candidate
after a single hit lets lucky passes through, and a 5/20 threshold is above what even the
unreduced workload achieves. The reducer now warns about this after the baseline, through
`policy_warning`. For low-rate targets, use `candidate_accept_hits` of 2 or more, and set a
final threshold consistent with the baseline rate.

## Later milestones (not implemented)

These are planned extensions; none is a prerequisite for the first release:

* Seeded timing and delay variation stored in the manifest.
* Explicit barrier and synchronization events.
* Token- or parser-aware SQL simplification.
* Setup and data reduction.
* Startup-option reduction.
* Parallel attempts with independent servers.
* Recovery incidents with crash, restart and check events.
* PXC and multi-node lifecycles.
* Optionally, structured before-send and completion events from pstress itself.
