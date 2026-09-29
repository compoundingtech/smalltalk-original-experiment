// tests/unit/convoy-layout.test.ts — regression guard for the convoy structure
// redesign (2026-07-17): the bus folder moves to
//   $XDG_STATE_HOME/convoy/<net>/smalltalk/<host>.<identity>/
// (named network + a synced `smalltalk/` subdir + HOST-PREFIXED folder names,
// with context/ inside the bus folder).
//
// st needs NO functional change for this: it is already fully parameterized on
// (ST_ROOT, ST_AGENT) and every per-agent path is `join(root, id, <sub>)`. This
// test PINS that contract so a future refactor can't silently break the layout
// convoy sets up: convoy points ST_ROOT at the `smalltalk/` subdir and sets
// ST_AGENT to the host-prefixed id; everything must resolve under it.

import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import {
  archiveDir,
  contextDecisionsDir,
  contextDir,
  contextNowPath,
  inboxDir,
  resourcesDir,
  statusPath,
  validAgent,
} from '../../src/common.ts';
import { getAgents } from '../../src/commands/agents.ts';

// ST_ROOT points at the synced `smalltalk/` subdir of a named network.
const ROOT = '/xdg/state/convoy/default/smalltalk';
// Agent ids are host-prefixed under the new layout.
const ID = 'example-mac.cos-claude';

describe('convoy redesign layout — host-prefixed ids are valid agents', () => {
  it('accepts host.identity forms (period-separated hierarchy)', () => {
    expect(validAgent('example-mac.cos-claude')).toBe(true);
    expect(validAgent('example-linux.app-web-claude')).toBe(true);
    expect(validAgent('example-mac.orchestrator.session-1.child-7')).toBe(true);
  });
});

describe('convoy redesign layout — every per-agent path resolves under ST_ROOT/<host.id>/', () => {
  it('inbox / archive / status / resources sit directly under the bus folder', () => {
    expect(inboxDir(ID, ROOT)).toBe(join(ROOT, ID, 'inbox'));
    expect(archiveDir(ID, ROOT)).toBe(join(ROOT, ID, 'archive'));
    expect(statusPath(ID, ROOT)).toBe(join(ROOT, ID, 'status'));
    expect(resourcesDir(ID, ROOT)).toBe(join(ROOT, ID, 'resources'));
  });

  it('context/ lives INSIDE the bus folder (now.md + decisions/)', () => {
    expect(contextDir(ID, ROOT)).toBe(join(ROOT, ID, 'context'));
    expect(contextNowPath(ID, ROOT)).toBe(join(ROOT, ID, 'context', 'now.md'));
    expect(contextDecisionsDir(ID, ROOT)).toBe(join(ROOT, ID, 'context', 'decisions'));
  });
});

describe('convoy redesign layout — enumeration lists host-prefixed bus folders', () => {
  let root: string;
  beforeEach(() => {
    // A real smalltalk/ subdir holding two host-prefixed agent folders.
    root = mkdtempSync(join(tmpdir(), 'convoy-layout-'));
    for (const id of ['example-mac.app-web-claude', 'example-linux.example-linux-codex']) {
      mkdirSync(join(root, id, 'inbox'), { recursive: true });
      mkdirSync(join(root, id, 'archive'), { recursive: true });
    }
    writeFileSync(join(root, 'example-mac.app-web-claude', 'status'), 'available');
  });
  afterEach(() => {
    rmSync(root, { recursive: true, force: true });
  });

  it('getAgents enumerates host-prefixed folders with their status', () => {
    const agents = getAgents(root);
    expect(agents.map((a) => a.identity).sort()).toEqual([
      'example-linux.example-linux-codex',
      'example-mac.app-web-claude',
    ]);
    const web = agents.find((a) => a.identity === 'example-mac.app-web-claude');
    expect(web?.status).toBe('available');
  });
});
