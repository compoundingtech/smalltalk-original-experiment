// tests/integration/cross-machine-ding.test.ts — cross-machine live-ding.
//
// Verifies that an ALIVE + AVAILABLE ding daemon fires a live [DING] poke
// for a fresh CROSS-MACHINE arrival — a message written the way `rsync -a`
// writes one: a temp file, an OLD preserved source-machine mtime, and an
// atomic rename into the inbox.
//
// This is the end-to-end rig behind the "cross-machine ding does not fire"
// investigation (fix 674af94). The observed field failure turned out to be
// an old-transport ROOT-MISMATCH (the daemon watched one ST_ROOT while the
// sync wrote the message into another), NOT a ding-code bug: the live-watch
// path pokes cross-machine arrivals in every scenario, as asserted below.
//
// Unlike the pty-based integration ding test, this drives `runDing`
// in-process with an injected PtySender (capture) + a stub session probe,
// so it needs no real `pty` and runs everywhere. It uses a real `createSt`
// watch over a real filesystem inbox — the parts that actually matter for
// the cross-machine question (the poll surfacing an old-mtime file).

import {
  mkdirSync,
  mkdtempSync,
  renameSync,
  rmSync,
  utimesSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { createSt } from '../../src/lib.ts';
import { runDing } from '../../src/commands/ding.ts';
import { asIdentity } from '../../src/types.ts';

let root: string;
const identity = asIdentity('cos.claude');

beforeEach(() => {
  root = mkdtempSync(join(tmpdir(), 'xm-ding-'));
  mkdirSync(join(root, identity, 'inbox'), { recursive: true });
  mkdirSync(join(root, identity, 'archive'), { recursive: true });
});

afterEach(() => {
  rmSync(root, { recursive: true, force: true });
});

/** Plant a message the way `rsync -a` delivers a cross-machine one: write a
 *  temp file, stamp it with an OLD source-machine mtime (rsync -a preserves
 *  the peer's write time, which skews below this machine's clock), then
 *  atomic-rename it into the inbox so it appears complete in one step. */
function plantCrossMachine(filename: string, ageMs: number): void {
  const inbox = join(root, identity, 'inbox');
  const tmp = join(inbox, `.tmp-${filename}`);
  writeFileSync(
    tmp,
    '---\nfrom: example-linux.bob\nsubject: cross-machine live test\n---\nhello from example-linux\n',
  );
  const old = new Date(Date.now() - ageMs);
  utimesSync(tmp, old, old);
  renameSync(tmp, join(inbox, filename));
}

/** Start an alive + available ding daemon with a captured PtySender, rescan
 *  / tidy / status-refresh all disabled so ONLY the live watch can fire a
 *  poke. Returns the capture array plus an abortable stop(). */
function startLiveDing(): {
  pokes: string[][];
  stop: () => Promise<void>;
} {
  const pokes: string[][] = [];
  const ac = new AbortController();
  const done = runDing({
    st: createSt({ root, identity }),
    identity,
    ptySession: 'fake',
    ptySend: async (_s, seqs) => {
      pokes.push([...seqs]);
      return { status: 0, stderr: '' };
    },
    ptyPeek: async () => ({ status: 0, stdout: 'quiet', stderr: '' }),
    isSessionAlive: () => true,
    paneGuard: false, // deliver on arrival — isolate the watch->poke path
    exitWhenSessionGone: false,
    rescanIntervalMs: 0, // no rescan tick: prove the LIVE watch fires
    tidyIntervalMs: 0,
    statusRefreshIntervalMs: 0,
    intervalMs: 100,
    signal: ac.signal,
  }).catch(() => {
    /* aborted teardown resolves via signal; swallow */
  });
  return {
    pokes,
    stop: async () => {
      ac.abort();
      await done;
    },
  };
}

async function waitForPokes(
  pokes: string[][],
  count: number,
  timeoutMs = 4000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (pokes.length >= count) return;
    await new Promise((r) => setTimeout(r, 50));
  }
}

describe('cross-machine live ding', () => {
  it(
    'pokes a fresh cross-machine (old-mtime, atomic-rename) arrival mid-session',
    async () => {
      const st = createSt({ root, identity });
      await st.setStatus(identity, 'available');

      const ding = startLiveDing();
      try {
        // Let the watch arm (replay cutoff = now) and settle past a poll.
        await new Promise((r) => setTimeout(r, 500));
        expect(ding.pokes.length).toBe(0);

        plantCrossMachine(`${Date.now()}-xmxm01.md`, 2 * 60 * 60_000);

        await waitForPokes(ding.pokes, 1);
        expect(ding.pokes.length).toBe(1);
        // The poke carries the correct [DING] text + a trailing Enter.
        expect(ding.pokes[0][0]).toContain('[DING] new smalltalk message');
        expect(ding.pokes[0][0]).toContain('from example-linux.bob');
        expect(ding.pokes[0]).toContain('key:return');
      } finally {
        await ding.stop();
      }
    },
    15_000,
  );

  it(
    'pokes a cross-machine arrival that lands in the startup replay-window (old mtime does not strand it)',
    async () => {
      const st = createSt({ root, identity });
      await st.setStatus(identity, 'available');

      const ding = startLiveDing();
      try {
        // Plant almost immediately — during arm/replay. The poll is
        // filename-keyed (no mtime cutoff), so the old mtime must not
        // exclude it the way the startup mtime-gate once did.
        await new Promise((r) => setTimeout(r, 30));
        plantCrossMachine(`${Date.now()}-xmxm02.md`, 2 * 60 * 60_000);

        await waitForPokes(ding.pokes, 1);
        expect(ding.pokes.length).toBeGreaterThanOrEqual(1);
        expect(ding.pokes[0][0]).toContain('from example-linux.bob');
      } finally {
        await ding.stop();
      }
    },
    15_000,
  );
});
