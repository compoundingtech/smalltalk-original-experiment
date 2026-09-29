// commands/sync-fabric.ts — `st sync fabric`, the cross-machine bus syncer.
//
// Productizes the naked "rsync-over-a-fabric-dialed-socket" bus loop into one
// clean supervisable unit per side (see docs/design). Two subverbs:
//
//   st sync fabric run <peer>    the CLIENT syncer loop (this file, live today)
//   st sync fabric serve         the EXPOSE side — GATED on fabric's exec-expose
//                                --exec contract (registers a per-dial
//                                `rsync --server --daemon`; no resident proc)
//
// Per the network decision, fabric is the ONLY thing that stays resident: the
// serve side spawns rsync on-demand per dial, and the periodic tombstone sweep
// lives entirely in the RUN loop below (it sweeps BOTH roots — local directly,
// and the remote via a targeted, twin-verified delete). No serve-side sweep
// proc, no socat, zero naked smalltalk procs on the exposing box.
//
// The rsync invocation is injected via `deps.runRsync` (same shape as
// commands/sync.ts) so the cycle is unit-testable without a live bus.

import { spawnSync } from 'node:child_process';
import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  watch as fsWatch,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import { invokedName, type CliContext } from '../cli-context.ts';
import { applySweepPlan, sweepPlan } from '../common.ts';
import { SyncFailedError } from '../errors.ts';
import {
  resolvePeer,
  type RsyncResult,
  type SyncContext,
} from './sync.ts';

// ─── scope ──────────────────────────────────────────────────────────────
//
// Default scope = message traffic + cross-machine liveness:
//   `*/inbox/**` + `*/archive/**`  (messages)
//   `*/status`                     (the liveness heartbeat: mtime = alive/dead)
// Each agent's `status` file syncs so a remote reader sees real live/offline
// from its mtime. This is mtime-PRESERVING (rsync `-a` implies `-t`), so a
// dead agent's frozen status mtime propagates as dead rather than being
// refreshed to receive-time — the load-bearing property (see the pin test).
// Home-host-authoritative union (no `--delete`): each host writes only its own
// agents' status, so no two hosts write the same file → no flap.
//
// HOST is NOT a synced file: under the convoy redesign the bus folder is named
// `<host>.<identity>`, so host is derived from the folder-name prefix by the
// reader (no `<id>/host` marker to write or sync). `context` files stay
// machine-local and are never synced unless `--scope all`.

/** rsync include/exclude args for the default scope: inbox+archive subtrees
 *  plus the per-agent `status` liveness file. */
export const SCOPE_DEFAULT: readonly string[] = [
  '--prune-empty-dirs',
  '--include=*/',
  '--include=inbox/**',
  '--include=archive/**',
  '--include=*/status',
  '--exclude=*',
];

/** rsync include/exclude args that limit a transfer to inbox subtrees only.
 * Used to bound the `--delete` blast radius of the remote sweep to inboxes. */
export const SCOPE_INBOX_ONLY: readonly string[] = [
  '--prune-empty-dirs',
  '--include=*/',
  '--include=inbox/**',
  '--exclude=*',
];

export type SyncScope = 'inbox-archive' | 'all';

function scopeFilters(scope: SyncScope): readonly string[] {
  return scope === 'all' ? [] : SCOPE_DEFAULT;
}

// ─── the safe remote sweep (the crux) ───────────────────────────────────

/**
 * Build the rsync filter args that delete EXACTLY the given root-relative
 * paths from the remote inbox, protecting everything else from deletion.
 *
 * The mechanism is `--delete` narrowed by protect/risk rules. rsync's
 * `--delete` removes destination files absent from the source; on its own
 * that is UNSAFE here, because a fresh, not-yet-reconciled delivery on the
 * remote is also absent from our (already-swept) source and would be
 * silently eaten — worse than leaving a stale one. So we protect ALL
 * destination files with `P **` and then un-protect (risk) ONLY the paths
 * we have positively verified are byte-identical archive twins (via
 * {@link sweepPlan}). A fresh delivery has no archive twin, so it is never
 * in `relPaths`; `P **` keeps it. There is no absence-based deletion.
 *
 * Rule order is significant: rsync evaluates filter rules first-match-wins,
 * so every specific `R` MUST precede the `P **` catch-all.
 *
 * Returns `[]` for an empty plan — the caller then skips the remote-sweep
 * rsync entirely (nothing to delete).
 */
export function remoteSweepFilters(relPaths: readonly string[]): string[] {
  if (relPaths.length === 0) return [];
  const args: string[] = ['--delete'];
  for (const rel of relPaths) {
    args.push(`--filter=R ${rel}`);
  }
  args.push('--filter=P **');
  return args;
}

// ─── one sync cycle ──────────────────────────────────────────────────────

export interface FabricCycleDeps {
  /** Runs `rsync -a <args...>`. Defaults to the real spawnSync invocation. */
  runRsync?: (args: string[]) => RsyncResult;
  /** Banner emitter (defaults to no-op; the loop wires stderr). */
  banner?: (line: string) => void;
}

export interface FabricCycleResult {
  /** how many redundant inbox files were removed LOCALLY this cycle. */
  localRemoved: number;
  /** the root-relative paths deleted from the REMOTE inbox this cycle. */
  remoteDeleted: string[];
}

/**
 * Run ONE convergence cycle against `remoteRoot` (an already-resolved rsync
 * target, e.g. `"peer::smalltalk/"` or a `local:` path). `transportArgs`
 * carries the transport (`['-e', <rsh>]` for a fabric-dialed socket; `[]`
 * for a local/ssh peer). Order matters:
 *
 *   1. PULL  remote -> local (union, scoped) — brings the peer's fresh
 *      deliveries AND its archive tombstones into our root.
 *   2. PLAN  the tombstone sweep from the just-updated local root, so it
 *      includes any twins that arrived in step 1.
 *   3. LOCAL sweep — remove those inbox copies here.
 *   4. REMOTE sweep — delete the SAME twin-verified paths from the peer's
 *      inbox (targeted; never absence-based — see {@link remoteSweepFilters}).
 *   5. PUSH  local -> remote (union, scoped, NO --delete) — sends our fresh
 *      deliveries and tombstones onward.
 *
 * A file delivered to the remote AFTER step 1 is untwinned, is not in the
 * step-2 plan, and is protected in step 4 — it survives and syncs next cycle.
 */
/** Default rsync I/O timeout (seconds). Without it, rsync waits forever for a
 * dead fabric tunnel, wedging a whole cycle (observed: an 8-minute hang on a
 * silently-dropped dial). With it, a stalled transfer errors out and the run
 * loop re-dials instead of hanging. */
export const IO_TIMEOUT_S = 45;

/** Seconds added to the rsync I/O timeout to get the wall-clock backstop.
 * rsync's own `--timeout` should normally win; this buffer just keeps the hard
 * kill from racing it on a healthy-but-slow transfer. */
export const HARD_TIMEOUT_BUFFER_S = 15;

/** The spawnSync wall-clock timeout (ms) for a fabric-cycle rsync. */
export function fabricHardTimeoutMs(ioTimeoutS: number): number {
  return (ioTimeoutS + HARD_TIMEOUT_BUFFER_S) * 1000;
}

/**
 * The fabric cycle's default rsync runner: `rsync -a -u <args>` via spawnSync
 * WITH a wall-clock `timeout`.
 *
 * `-u` (--update): skip any file that is NEWER on the receiver. Messages are
 * immutable so this is a no-op for them, but the liveness `status` file MUTATES
 * (its mtime bumps every heartbeat). Without `-u`, the pull leg (remote→local,
 * a plain `rsync -a`) would overwrite a home host's just-touched status with the
 * older copy it previously synced OUT — reverting the heartbeat and making a
 * live agent look stale. With `-u`, the home host (which always holds the newest
 * status, since only it writes that agent's file) wins, and a dead agent's
 * frozen mtime is never bumped by anyone. Newer-mtime-wins is exactly the
 * home-host-authoritative semantics.
 *
 * `timeout`: rsync's own `--timeout` is an I/O-inactivity timeout that does NOT
 * cover a connection wedged in the pre-transfer handshake — the exact state an
 * in-flight connection lands in when the serve backend is swapped or a fabric
 * path transitions mid-transfer. Such an rsync hangs forever, and because
 * spawnSync blocks, the whole cycle hangs and `cmdSyncFabricRun`'s
 * re-dial-on-error is never reached. The spawnSync `timeout` force-kills it
 * (status → null → treated as failure → the loop re-dials). Observed live: a
 * pull hung 25 min through a serve cutover with `--timeout` set but never firing.
 */
export function fabricRunRsync(ioTimeoutS: number): (args: string[]) => RsyncResult {
  const timeout = fabricHardTimeoutMs(ioTimeoutS);
  return (args) => {
    const r = spawnSync('rsync', ['-a', '-u', ...args], {
      stdio: ['inherit', 'inherit', 'pipe'],
      encoding: 'utf8',
      timeout,
    });
    return {
      status: r.status ?? -1,
      stderr: typeof r.stderr === 'string' ? r.stderr : undefined,
    };
  };
}

export function fabricSyncCycle(
  localRoot: string,
  remoteRoot: string,
  transportArgs: readonly string[],
  scope: SyncScope,
  deps: FabricCycleDeps = {},
  ioTimeoutS: number = IO_TIMEOUT_S
): FabricCycleResult {
  const runRsync = deps.runRsync ?? fabricRunRsync(ioTimeoutS);
  const banner = deps.banner ?? (() => {});
  const src = `${stripTrailingSlash(localRoot)}/`;
  const filters = scopeFilters(scope);
  // Prefix on every rsync: transport (e.g. -e rsh) + an I/O timeout so a dead
  // fabric tunnel fails the cycle fast instead of hanging it indefinitely.
  const prefix = [...transportArgs, `--timeout=${ioTimeoutS}`];

  // 1. PULL remote -> local.
  banner(`# fabric pull: ${remoteRoot} -> ${src}`);
  const pull = runRsync([...prefix, ...filters, remoteRoot, src]);
  if (pull.status !== 0) {
    throw new SyncFailedError(
      'pull',
      pull.status,
      pull.stderr,
      `fabric pull failed: ${remoteRoot} -> ${src}`
    );
  }

  // 2. PLAN + 3. LOCAL sweep (one snapshot, applied here and mirrored below).
  const plan = sweepPlan(localRoot);
  const localRemoved = applySweepPlan(plan);
  const rels = plan.map((e) => e.rel);

  // 4. REMOTE sweep — targeted, twin-verified deletion of the peer's copies.
  if (rels.length > 0) {
    banner(`# fabric remote-sweep: delete ${rels.length} twin(s) from ${remoteRoot}`);
    const rs = runRsync([
      ...prefix,
      ...remoteSweepFilters(rels),
      ...SCOPE_INBOX_ONLY,
      src,
      remoteRoot,
    ]);
    if (rs.status !== 0) {
      throw new SyncFailedError(
        'remote-sweep',
        rs.status,
        rs.stderr,
        `fabric remote-sweep failed: ${src} -> ${remoteRoot}`
      );
    }
  }

  // 5. PUSH local -> remote (union, no delete).
  banner(`# fabric push: ${src} -> ${remoteRoot}`);
  const push = runRsync([...prefix, ...filters, src, remoteRoot]);
  if (push.status !== 0) {
    throw new SyncFailedError(
      'push',
      push.status,
      push.stderr,
      `fabric push failed: ${src} -> ${remoteRoot}`
    );
  }

  return { localRemoved, remoteDeleted: rels };
}

function stripTrailingSlash(s: string): string {
  return s.endsWith('/') ? s.slice(0, -1) : s;
}

/**
 * The catalog cross-machine union-sync (declarative-convoy piece 2): pull then
 * push `<net>/catalog/` with the peer. Union-only (no `--delete`), newer-wins
 * (`-u`), mtime-preserving (`-t`) — the same machinery the liveness status sync
 * uses. Deliberately SIMPLE relative to the message-bus cycle: NO tombstone
 * sweep. A catalog `.toml` is not an inbox/archive twin; a decommission is a
 * `retired = true` EDIT that propagates like any other edit (an `rm` can't be
 * the removal primitive under no-delete union — a peer just re-propagates the
 * file). This is content-agnostic: it carries the whole flat catalog to every
 * machine; convoy's reconcile host-filters and honors `retired`.
 */
export function catalogUnionSync(
  localRoot: string,
  remoteRoot: string,
  transportArgs: readonly string[],
  ioTimeoutS: number,
  deps: FabricCycleDeps = {}
): void {
  const runRsync = deps.runRsync ?? fabricRunRsync(ioTimeoutS);
  const banner = deps.banner ?? (() => {});
  const src = `${stripTrailingSlash(localRoot)}/`;
  const prefix = [...transportArgs, `--timeout=${ioTimeoutS}`];

  banner(`# catalog pull: ${remoteRoot} -> ${src}`);
  const pull = runRsync([...prefix, remoteRoot, src]);
  if (pull.status !== 0) {
    throw new SyncFailedError(
      'pull',
      pull.status,
      pull.stderr,
      `catalog pull failed: ${remoteRoot} -> ${src}`
    );
  }
  banner(`# catalog push: ${src} -> ${remoteRoot}`);
  const push = runRsync([...prefix, src, remoteRoot]);
  if (push.status !== 0) {
    throw new SyncFailedError(
      'push',
      push.status,
      push.stderr,
      `catalog push failed: ${src} -> ${remoteRoot}`
    );
  }
}

// ─── transport hardening (decision b) ────────────────────────────────────
//
// The gotchas that cost us hours live in the tool, not tribal memory:
//  - macOS /usr/bin/rsync is openrsync, which can't do daemon-over-rsh.
//  - RSYNC_RSH env does NOT trigger daemon-over-rsh — you must pass `-e`.
//  - rsync's `-e` word-splits, so the rsh must be a wrapper SCRIPT that
//    execs socat and ignores the host/remote-cmd args rsync appends.
//  - macOS unix-socket paths are capped at 104 chars.

/** Throw a clear, actionable error if `rsync --version` output is openrsync. */
export function assertModernRsync(versionText: string): void {
  if (/openrsync/i.test(versionText)) {
    throw new Error(
      'fabric sync needs GNU rsync (for daemon-over-rsh); found openrsync — ' +
        "macOS's /usr/bin/rsync. Install a modern rsync (e.g. `brew install " +
        'rsync`) and put it ahead of /usr/bin on PATH.'
    );
  }
}

/** macOS `sun_path` limit; socat fails to connect a longer path. */
export const MAX_UNIX_SOCKET_PATH = 104;

/** Throw if a fabric-dialed socket path exceeds the unix-socket limit. */
export function assertSocketPathOk(socketPath: string): void {
  if (socketPath.length > MAX_UNIX_SOCKET_PATH) {
    throw new Error(
      `fabric dial socket path is ${socketPath.length} chars, over the ` +
        `${MAX_UNIX_SOCKET_PATH}-char unix-socket limit — socat cannot ` +
        `connect it: ${socketPath}`
    );
  }
}

/**
 * The rsh wrapper script rsync's `-e` invokes. rsync calls it as
 * `<script> <host> rsync --server --daemon …`; the script ignores those
 * trailing args and just bridges stdin/stdout to the fabric-dialed unix
 * socket, so `rsync … <token>::<module>/` speaks the rsync daemon protocol
 * over fabric. (socat would treat the appended args as extra address specs,
 * hence the wrapper rather than a bare `-e 'socat …'`.)
 */
export function rshScriptContent(socketPath: string): string {
  return `#!/bin/sh\n# generated by \`st sync fabric run\` — bridges rsync to a\n# fabric-dialed unix socket. Args from rsync are intentionally ignored.\nexec socat - UNIX-CONNECT:${socketPath}\n`;
}

// ─── run-side transport setup + loop ─────────────────────────────────────

/** rsync daemon module the serve side exposes: the whole NETWORK dir
 *  (`<net>/`), so both the synced subtrees — `smalltalk/` (the message bus)
 *  and `catalog/` (the declarative-convoy scheduler) — are reachable as
 *  sub-paths of one module. `pty/` + `worktrees/` live under it too but are
 *  never synced (the run side never targets them; the serve conf also excludes
 *  them). */
export const RSYNC_MODULE = 'net';
/** fabric protocol name the serve side exposes / the run side dials. */
export const FABRIC_PROTO = 'st-sync';

interface Transport {
  /** The NET-level rsync target base, e.g. `"peer::net/"` or `"/path/net/"`.
   *  The run side appends `smalltalk/` and `catalog/` to reach each subtree. */
  remoteBase: string;
  /** rsync transport args: `['-e', <rsh>]` for fabric; `[]` otherwise. */
  transportArgs: string[];
}

/**
 * Resolve a peer spec to an rsync transport.
 * - `fabric:<peer>` → dial fabric for a unix socket, write the rsh wrapper,
 *   and target the daemon module over daemon-over-rsh.
 * - anything else (`local:` / `host:path` / bare / alias) → reuse the
 *   standard {@link resolvePeer}, plain rsync, no transport args.
 */
function setUpTransport(
  peer: string,
  ctx: CliContext,
  sctx: SyncContext
): Transport {
  if (!peer.startsWith('fabric:')) {
    return { remoteBase: resolvePeer(peer, sctx), transportArgs: [] };
  }
  const fabricPeer = peer.slice('fabric:'.length);
  if (fabricPeer.length === 0) {
    throw new Error('fabric: peer requires a name, e.g. fabric:example-host');
  }
  // `fabric dial <peer> <proto>` prints the local socket path and exits; the
  // fabric daemon keeps the socket alive.
  const dial = spawnSync('fabric', ['dial', fabricPeer, FABRIC_PROTO], {
    encoding: 'utf8',
  });
  if (dial.status !== 0) {
    throw new Error(
      `fabric dial ${fabricPeer} ${FABRIC_PROTO} failed` +
        (dial.stderr ? `: ${dial.stderr.trim()}` : '')
    );
  }
  const socketPath = (dial.stdout ?? '').trim();
  if (socketPath.length === 0) {
    throw new Error(`fabric dial ${fabricPeer} returned no socket path`);
  }
  assertSocketPathOk(socketPath);
  const scriptDir = mkdtempSync(join(tmpdir(), 'st-fab-rsh-'));
  const scriptPath = join(scriptDir, 'rsh.sh');
  writeFileSync(scriptPath, rshScriptContent(socketPath));
  chmodSync(scriptPath, 0o755);
  return {
    remoteBase: `${fabricPeer}::${RSYNC_MODULE}/`,
    transportArgs: ['-e', scriptPath],
  };
}

/** Append a subtree to a net-level rsync base target, e.g.
 *  `("peer::net/", "smalltalk")` → `"peer::net/smalltalk/"`. */
function remoteSubtree(remoteBase: string, subtree: string): string {
  const base = remoteBase.endsWith('/') ? remoteBase : `${remoteBase}/`;
  return `${base}${subtree}/`;
}

// ─── serve side (fabric exec-expose) ─────────────────────────────────────
//
// Per the LOCKED fabric --exec contract: fabric spawns the exec command ONCE
// per tunnel session, pipes stdin/stdout (stderr -> daemon log), and the child
// exits on stdin EOF. `rsync --server --daemon` already speaks stdin/stdout, so
// fabric spawns one rsync per dial — no socat, no persistent rsyncd. `serve`
// therefore just writes the module config and registers the exec-expose; it is
// a near-zero-resident registration, NOT a loop. The fabric daemon is the only
// thing that stays up.

/**
 * rsyncd module config served per-dial by `rsync --server --daemon --config`.
 * The module path is the NETWORK dir (`<net>/`), so both `smalltalk/` and
 * `catalog/` are reachable sub-paths. `read only = false` so the run side can
 * push (and remote-sweep). `use chroot = false` because fabric already isolates
 * the tunnel and we serve a user-owned state tree, not a system path. The
 * `exclude` keeps `pty/` + `worktrees/` machine-local — the daemon refuses to
 * serve them even if a client asked (defense-in-depth; the run side never
 * targets them either).
 */
export function rsyncdConfContent(netRoot: string, module: string = RSYNC_MODULE): string {
  return (
    'use chroot = false\n' +
    'max verbosity = 1\n' +
    `[${module}]\n` +
    `    path = ${netRoot}\n` +
    '    read only = false\n' +
    '    munge symlinks = no\n' +
    '    exclude = /pty/ /worktrees/\n'
  );
}

/** The `fabric expose` argv that registers the per-dial rsync exec handler. */
export function fabricExposeArgs(confPath: string): string[] {
  return [
    'expose',
    FABRIC_PROTO,
    '--exec',
    '--',
    'rsync',
    '--server',
    '--daemon',
    `--config=${confPath}`,
    '.',
  ];
}

export interface FabricServeOptions {
  root: string;
  confPath: string;
}

/**
 * Register the serve side: write the rsyncd module config, then register the
 * fabric exec-expose that spawns `rsync --server --daemon` per dial. Returns
 * after registration (fabric holds the exposure); nothing stays resident here.
 */
export function cmdSyncFabricServe(
  opts: FabricServeOptions,
  ctx: CliContext
): number {
  // opts.root is ST_ROOT (`<net>/smalltalk/`); expose the NETWORK dir above it
  // so both smalltalk/ and catalog/ are reachable sub-paths of the module.
  const netRoot = dirname(stripTrailingSlash(opts.root));
  mkdirSync(dirname(opts.confPath), { recursive: true });
  writeFileSync(opts.confPath, rsyncdConfContent(netRoot));
  const args = fabricExposeArgs(opts.confPath);
  const r = spawnSync('fabric', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
  if (r.error !== undefined && (r.error as NodeJS.ErrnoException).code === 'ENOENT') {
    throw new Error('fabric not found on PATH (serve needs fabric exec-expose)');
  }
  if (r.status !== 0) {
    throw new Error(
      `fabric expose ${FABRIC_PROTO} failed` +
        (r.stderr ? `: ${String(r.stderr).trim()}` : '')
    );
  }
  ctx.stderr(
    `# fabric sync serving ${RSYNC_MODULE} (${netRoot}) via exec-expose ${FABRIC_PROTO}\n` +
      `# config: ${opts.confPath} — fabric spawns rsync per dial (no resident proc)\n`
  );
  return 0;
}

/** Verify a modern rsync is on PATH (rejects openrsync — decision b). */
function checkRsync(): void {
  const v = spawnSync('rsync', ['--version'], { encoding: 'utf8' });
  if (v.status !== 0) {
    throw new Error('rsync not found on PATH (fabric sync needs GNU rsync)');
  }
  assertModernRsync(`${v.stdout ?? ''}${v.stderr ?? ''}`);
}

// ─── push-on-change (item 4) ─────────────────────────────────────────────
//
// A best-effort accelerator on top of the interval poll: wake the loop early
// when a change genuinely warrants an immediate sync, so a status flip
// (available→busy) or a new message crosses machines in ~ms instead of waiting
// the poll interval. CRITICAL (per the fabric-churn constraint): it must NOT
// fire on the 30s status HEARTBEAT — a heartbeat re-writes the same content
// (mtime-only bump), and ~20 agents × heartbeats would otherwise turn into a
// near-continuous sync over the transport. So status events are CONTENT-gated;
// heartbeats ride the normal interval poll. It never gates correctness: the
// interval always runs, so a missed/unsupported watch only loses the speedup.

export interface PushGateState {
  /** Last-seen content per status-file rel-path, for the content gate. */
  lastStatusContent: Map<string, string>;
}

export function newPushGateState(): PushGateState {
  return { lastStatusContent: new Map() };
}

/**
 * Decide whether a change at `relPath` (relative to ST_ROOT) warrants an
 * immediate push-on-change sync. `readStatus(rel)` returns a status file's
 * current content (or undefined if unreadable); it is only consulted for
 * status paths. A new inbox/archive file triggers; a status CONTENT flip
 * triggers; a status heartbeat (same content, mtime-only bump) does NOT;
 * everything else (context, etc.) is machine-local and ignored.
 */
export function shouldPushSync(
  relPath: string,
  readStatus: (rel: string) => string | undefined,
  state: PushGateState
): boolean {
  if (/(^|\/)(inbox|archive)\//.test(relPath)) return true;
  if (/(^|\/)status$/.test(relPath)) {
    const now = readStatus(relPath);
    if (now === undefined) return false;
    const prev = state.lastStatusContent.get(relPath);
    state.lastStatusContent.set(relPath, now);
    // First-seen doesn't trigger (it rides the interval); only a real change
    // to already-tracked content is a state flip worth an immediate sync.
    return prev !== undefined && prev !== now;
  }
  return false;
}

export interface PushWatcher {
  /** Resolve after `ms`, OR early when a gated change fires (debounced). */
  wait(ms: number): Promise<void>;
  close(): void;
}

/**
 * Watch `root` recursively and let {@link PushWatcher.wait} resolve early on a
 * gated change (see {@link shouldPushSync}). Best-effort: if the OS recursive
 * watch is unsupported or errors, `wait(ms)` simply times out normally — the
 * caller's interval poll still runs, so push-on-change only ACCELERATES.
 */
export function createPushWatcher(root: string, debounceMs = 200): PushWatcher {
  const state = newPushGateState();
  let wake: (() => void) | null = null;
  let debounce: ReturnType<typeof setTimeout> | null = null;
  const fire = (): void => {
    if (debounce) clearTimeout(debounce);
    debounce = setTimeout(() => {
      debounce = null;
      if (wake) {
        const w = wake;
        wake = null;
        w();
      }
    }, debounceMs);
  };
  let watcher: ReturnType<typeof fsWatch> | undefined;
  try {
    watcher = fsWatch(root, { recursive: true }, (_evt, filename) => {
      if (typeof filename !== 'string') return;
      const rel = filename;
      const read = (p: string): string | undefined => {
        try {
          return readFileSync(join(root, p), 'utf8');
        } catch {
          return undefined;
        }
      };
      if (shouldPushSync(rel, read, state)) fire();
    });
    watcher.on('error', () => {
      /* best-effort — a failed watch just falls back to interval polling */
    });
  } catch {
    /* recursive watch unsupported here → no watcher; wait() times out */
  }
  return {
    wait(ms: number): Promise<void> {
      return new Promise((resolve) => {
        const t = setTimeout(() => {
          wake = null;
          resolve();
        }, ms);
        wake = (): void => {
          clearTimeout(t);
          wake = null;
          resolve();
        };
      });
    },
    close(): void {
      if (debounce) clearTimeout(debounce);
      try {
        watcher?.close();
      } catch {
        /* ignore */
      }
    },
  };
}

export interface FabricRunOptions {
  peer: string;
  root: string;
  scope: SyncScope;
  intervalMs: number;
  once: boolean;
  /** rsync I/O timeout (seconds) — a dead tunnel fails the cycle fast. */
  timeoutS: number;
}

/**
 * The `st sync fabric run` loop: set up transport, then run convergence
 * cycles until interrupted. On a cycle error, it re-establishes the
 * transport (a fabric dial can drop) and backs off before retrying — a drop
 * locks no one out, since this is the client and the peer holds no resident
 * proc. `--once` runs a single cycle (cron-style / test hook) and exits.
 */
export async function cmdSyncFabricRun(
  opts: FabricRunOptions,
  ctx: CliContext
): Promise<number> {
  checkRsync();
  const sctx: SyncContext = { stRoot: opts.root, stConfig: ctx.stConfig };
  const deps: FabricCycleDeps = { banner: (l) => ctx.stderr(`${l}\n`) };

  // opts.root is ST_ROOT (`<net>/smalltalk/`); the catalog subtree is its
  // sibling `<net>/catalog/`. Each pass targets the matching sub-path of the
  // peer's `<net>/` module.
  const catalogRoot = join(dirname(stripTrailingSlash(opts.root)), 'catalog');

  // One cycle = PASS 1, the smalltalk message-bus cycle (unchanged, WITH its
  // tombstone sweep), then PASS 2, the catalog union-sync (no sweep; only if
  // catalog/ exists — it doesn't under the pre-redesign layout).
  const runCycle = (t: Transport): FabricCycleResult => {
    const r = fabricSyncCycle(
      opts.root,
      remoteSubtree(t.remoteBase, 'smalltalk'),
      t.transportArgs,
      opts.scope,
      deps,
      opts.timeoutS
    );
    if (existsSync(catalogRoot)) {
      catalogUnionSync(
        catalogRoot,
        remoteSubtree(t.remoteBase, 'catalog'),
        t.transportArgs,
        opts.timeoutS,
        deps
      );
    }
    return r;
  };

  if (opts.once) {
    const t = setUpTransport(opts.peer, ctx, sctx);
    const r = runCycle(t);
    ctx.stderr(
      `# fabric cycle: swept ${r.localRemoved} local, ` +
        `${r.remoteDeleted.length} remote\n`
    );
    return 0;
  }

  // push-on-change: wakes the interval early on a real change (new message or
  // a status content flip); heartbeats ride the interval. Best-effort accelerator.
  // (Catalog changes ride the interval poll — well within scheduler latency.)
  const watcher = createPushWatcher(opts.root);
  let transport: Transport | undefined;
  for (;;) {
    try {
      if (transport === undefined) transport = setUpTransport(opts.peer, ctx, sctx);
      runCycle(transport);
    } catch (err) {
      ctx.stderr(`# fabric cycle error: ${errMsg(err)} — re-dialing\n`);
      transport = undefined; // force a fresh dial next iteration
    }
    await watcher.wait(opts.intervalMs);
  }
}

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

// ─── CLI ─────────────────────────────────────────────────────────────────

const DEFAULT_INTERVAL_S = 5;

export function cmdSyncFabricCli(
  args: readonly string[],
  ctx: CliContext
): Promise<number> | number {
  const name = invokedName(ctx.env);
  const usage =
    `usage: ${name} sync fabric run <peer> [--root PATH] [--scope inbox-archive|all]\n` +
    `                              [--interval S] [--timeout S] [--once]\n` +
    `       ${name} sync fabric serve [--root PATH] [--conf PATH]\n\n` +
    '  run   drive the cross-machine bus: dial <peer> (use fabric:<name> for a\n' +
    '        fabric peer), then loop pull -> sweep both roots -> push. --once\n' +
    '        runs a single cycle. Default scope is inbox+archive.\n' +
    '  serve expose this root: write the rsyncd module config and register a\n' +
    '        fabric exec-expose that spawns rsync per dial (no resident proc).\n';

  const sub = args[0];
  if (sub === undefined || sub === '-h' || sub === '--help') {
    ctx.stderr(usage);
    return sub === undefined ? 1 : 0;
  }

  const rest = args.slice(1);
  switch (sub) {
    case 'run':
      return runFromArgs(rest, ctx, usage);
    case 'serve':
      return serveFromArgs(rest, ctx, usage);
    default:
      ctx.stderr(`unknown subcommand: ${sub}\n${usage}`);
      return 1;
  }
}

function runFromArgs(
  rest: readonly string[],
  ctx: CliContext,
  usage: string
): Promise<number> {
  let peer: string | undefined;
  let root = ctx.stRoot;
  let scope: SyncScope = 'inbox-archive';
  let intervalMs = DEFAULT_INTERVAL_S * 1000;
  let timeoutS = IO_TIMEOUT_S;
  let once = false;

  for (let i = 0; i < rest.length; i++) {
    const a = rest[i]!;
    switch (a) {
      case '--once':
        once = true;
        break;
      case '--root':
      case '--st-root':
        root = expectValue(rest, ++i, a);
        break;
      case '--scope': {
        const v = expectValue(rest, ++i, a);
        if (v !== 'inbox-archive' && v !== 'all') {
          throw new Error(`--scope must be inbox-archive or all, got: ${v}`);
        }
        scope = v;
        break;
      }
      case '--interval': {
        const v = expectValue(rest, ++i, a);
        const s = Number(v);
        if (!Number.isFinite(s) || s <= 0) {
          throw new Error(`--interval must be a positive number of seconds, got: ${v}`);
        }
        intervalMs = Math.round(s * 1000);
        break;
      }
      case '--timeout': {
        const v = expectValue(rest, ++i, a);
        const s = Number(v);
        if (!Number.isFinite(s) || s <= 0) {
          throw new Error(`--timeout must be a positive number of seconds, got: ${v}`);
        }
        timeoutS = Math.round(s);
        break;
      }
      case '-h':
      case '--help':
        ctx.stderr(usage);
        return Promise.resolve(0);
      default:
        if (a.startsWith('-')) throw new Error(`unknown flag: ${a}`);
        if (peer === undefined) peer = a;
        else throw new Error(`unexpected arg: ${a}`);
    }
  }
  if (peer === undefined) throw new Error('<peer> required for run');
  return cmdSyncFabricRun({ peer, root, scope, intervalMs, timeoutS, once }, ctx);
}

function serveFromArgs(
  rest: readonly string[],
  ctx: CliContext,
  usage: string
): number {
  let root = ctx.stRoot;
  let confPath = join(ctx.stConfig, 'st-sync-rsyncd.conf');
  for (let i = 0; i < rest.length; i++) {
    const a = rest[i]!;
    switch (a) {
      case '--root':
      case '--st-root':
        root = expectValue(rest, ++i, a);
        break;
      case '--conf':
        confPath = expectValue(rest, ++i, a);
        break;
      case '-h':
      case '--help':
        ctx.stderr(usage);
        return 0;
      default:
        throw new Error(`unexpected arg: ${a}`);
    }
  }
  return cmdSyncFabricServe({ root, confPath }, ctx);
}

function expectValue(args: readonly string[], i: number, flag: string): string {
  const v = args[i];
  if (v === undefined) throw new Error(`${flag} requires a value`);
  return v;
}
