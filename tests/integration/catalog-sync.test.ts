// tests/integration/catalog-sync.test.ts — the catalog cross-machine union-sync
// (declarative-convoy piece 2) over real rsync, and the two-pass net-dir sync
// proving pty/ + worktrees/ never cross machines.

import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  statSync,
  utimesSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import {
  catalogUnionSync,
  fabricSyncCycle,
} from '../../src/commands/sync-fabric.ts';
import { modernRsyncAvailable } from './helpers.ts';

const d = modernRsyncAvailable() ? describe : describe.skip;
const OLD_MTIME_S = 1_600_000_000; // fixed old epoch
const mtimeS = (p: string): number => Math.floor(statSync(p).mtimeMs / 1000);

d('catalog union-sync (real rsync)', () => {
  let localCat: string;
  let remoteCat: string;

  beforeEach(() => {
    localCat = mkdtempSync(join(tmpdir(), 'catalog-local-'));
    remoteCat = mkdtempSync(join(tmpdir(), 'catalog-remote-'));
  });
  afterEach(() => {
    rmSync(localCat, { recursive: true, force: true });
    rmSync(remoteCat, { recursive: true, force: true });
  });

  it('carries a catalog file across, MTIME-PRESERVING', () => {
    writeFileSync(join(localCat, 'example-mac.cos-claude.toml'), 'host = "example-mac"\nretired = false\n');
    utimesSync(join(localCat, 'example-mac.cos-claude.toml'), OLD_MTIME_S, OLD_MTIME_S);

    catalogUnionSync(localCat, `${remoteCat}/`, [], 30);

    expect(readFileSync(join(remoteCat, 'example-mac.cos-claude.toml'), 'utf8')).toContain('host = "example-mac"');
    // Source mtime preserved, not receive-time (same guarantee as the bus).
    expect(mtimeS(join(remoteCat, 'example-mac.cos-claude.toml'))).toBe(OLD_MTIME_S);
  });

  it('newer-wins: a retired=true EDIT propagates (decommission is an edit, not an rm)', () => {
    writeFileSync(join(localCat, 'a.toml'), 'retired = false\n');
    utimesSync(join(localCat, 'a.toml'), OLD_MTIME_S, OLD_MTIME_S);
    catalogUnionSync(localCat, `${remoteCat}/`, [], 30);
    expect(readFileSync(join(remoteCat, 'a.toml'), 'utf8')).toContain('retired = false');

    // Decommission = edit the file (newer mtime), NOT delete it.
    const nowS = Math.floor(Date.now() / 1000);
    writeFileSync(join(localCat, 'a.toml'), 'retired = true\n');
    utimesSync(join(localCat, 'a.toml'), nowS, nowS);
    catalogUnionSync(localCat, `${remoteCat}/`, [], 30);

    expect(readFileSync(join(remoteCat, 'a.toml'), 'utf8')).toContain('retired = true');
  });

  it('union: a remote-only catalog file lands locally, and nothing is deleted', () => {
    writeFileSync(join(remoteCat, 'b.toml'), 'host = "example-linux"\n');
    writeFileSync(join(localCat, 'c.toml'), 'host = "example-mac"\n');

    catalogUnionSync(localCat, `${remoteCat}/`, [], 30);

    // Union: both sides end up with both files (no --delete).
    expect(existsSync(join(localCat, 'b.toml'))).toBe(true); // pulled to local
    expect(existsSync(join(remoteCat, 'c.toml'))).toBe(true); // pushed to remote
    expect(existsSync(join(remoteCat, 'b.toml'))).toBe(true); // remote's own kept
  });
});

d('two-pass net-dir sync: smalltalk/ + catalog/ sync, pty/ + worktrees/ never do', () => {
  let localNet: string;
  let remoteNet: string;

  beforeEach(() => {
    localNet = mkdtempSync(join(tmpdir(), 'net-local-'));
    remoteNet = mkdtempSync(join(tmpdir(), 'net-remote-'));
  });
  afterEach(() => {
    rmSync(localNet, { recursive: true, force: true });
    rmSync(remoteNet, { recursive: true, force: true });
  });

  it('runs both passes on the sub-paths; local-only pty/ + worktrees/ are not synced', () => {
    // Local net dir with all four subtrees.
    mkdirSync(join(localNet, 'smalltalk', 'bob', 'inbox'), { recursive: true });
    mkdirSync(join(localNet, 'smalltalk', 'bob', 'archive'), { recursive: true });
    writeFileSync(join(localNet, 'smalltalk', 'bob', 'status'), 'available');
    mkdirSync(join(localNet, 'catalog'), { recursive: true });
    writeFileSync(join(localNet, 'catalog', 'bob.toml'), 'host = "example-mac"\n');
    mkdirSync(join(localNet, 'pty'), { recursive: true });
    writeFileSync(join(localNet, 'pty', 'sess.pid'), 'machine-local');
    mkdirSync(join(localNet, 'worktrees'), { recursive: true });
    writeFileSync(join(localNet, 'worktrees', 'wt.txt'), 'machine-local');
    // Remote net dir: the two synced subtrees exist (as convoy init creates them).
    mkdirSync(join(remoteNet, 'smalltalk', 'bob', 'inbox'), { recursive: true });
    mkdirSync(join(remoteNet, 'smalltalk', 'bob', 'archive'), { recursive: true });
    mkdirSync(join(remoteNet, 'catalog'), { recursive: true });

    // The run loop's two passes, against the matching remote sub-paths.
    fabricSyncCycle(
      join(localNet, 'smalltalk'),
      `${join(remoteNet, 'smalltalk')}/`,
      [],
      'inbox-archive'
    );
    catalogUnionSync(join(localNet, 'catalog'), `${join(remoteNet, 'catalog')}/`, [], 30);

    // smalltalk/ + catalog/ crossed.
    expect(existsSync(join(remoteNet, 'smalltalk', 'bob', 'status'))).toBe(true);
    expect(existsSync(join(remoteNet, 'catalog', 'bob.toml'))).toBe(true);
    // pty/ + worktrees/ did NOT — neither pass targets them.
    expect(existsSync(join(remoteNet, 'pty'))).toBe(false);
    expect(existsSync(join(remoteNet, 'worktrees'))).toBe(false);
  });
});
