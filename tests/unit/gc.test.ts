// tests/unit/gc.test.ts — `st gc serve` tombstone garbage-collector service.
//
// The long-running loop + signal handling is exercised end-to-end elsewhere;
// here we cover the pure/observable surface: the `--once` sweep, the
// wrong-root WARN, and CLI arg parsing/dispatch.

import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { cmdGcServe, cmdGcCli } from '../../src/commands/gc.ts';
import type { CliContext } from '../../src/cli-context.ts';

let scratch: string;
let stRoot: string;
let stdoutBuf: string;
let stderrBuf: string;
let ctx: CliContext;

beforeEach(() => {
  scratch = mkdtempSync(join(tmpdir(), 'st-gc-test-'));
  stRoot = join(scratch, 'bus');
  mkdirSync(stRoot, { recursive: true });
  stdoutBuf = '';
  stderrBuf = '';
  ctx = {
    env: { ST_AGENT: 'example-mac.alice' },
    stRoot,
    stConfig: '/unused',
    stdout: (s) => {
      stdoutBuf += s;
    },
    stderr: (s) => {
      stderrBuf += s;
    },
    readStdin: async () => Buffer.alloc(0),
  };
});
afterEach(() => {
  rmSync(scratch, { recursive: true, force: true });
});

function setupAgent(root: string, id: string): void {
  mkdirSync(join(root, id, 'inbox'), { recursive: true });
  mkdirSync(join(root, id, 'archive'), { recursive: true });
}

const twin = '1714826789010-aaaaaa.md';
const fresh = '1714826789020-bbbbbb.md';

function seedZombieAndFresh(root: string, id: string): void {
  setupAgent(root, id);
  writeFileSync(join(root, id, 'inbox', twin), 'same');
  writeFileSync(join(root, id, 'archive', twin), 'same');
  writeFileSync(join(root, id, 'inbox', fresh), 'live');
}

describe('cmdGcServe — --once', () => {
  it('sweeps the resurrected twin, preserves the fresh message, returns 0', async () => {
    seedZombieAndFresh(stRoot, 'example-mac.alice');
    const rc = await cmdGcServe({ root: stRoot, intervalMs: 2000, once: true }, ctx);
    expect(rc).toBe(0);
    expect(stderrBuf).toContain('# gc: swept 1 redundant inbox file(s)');
    expect(existsSync(join(stRoot, 'example-mac.alice', 'inbox', twin))).toBe(false);
    expect(existsSync(join(stRoot, 'example-mac.alice', 'inbox', fresh))).toBe(true);
    // archive twin is the surviving tombstone
    expect(existsSync(join(stRoot, 'example-mac.alice', 'archive', twin))).toBe(true);
  });

  it('clean bus → swept 0, no removals', async () => {
    setupAgent(stRoot, 'example-mac.alice');
    writeFileSync(join(stRoot, 'example-mac.alice', 'inbox', fresh), 'live');
    const rc = await cmdGcServe({ root: stRoot, intervalMs: 2000, once: true }, ctx);
    expect(rc).toBe(0);
    expect(stderrBuf).toContain('# gc: swept 0 redundant inbox file(s)');
    expect(existsSync(join(stRoot, 'example-mac.alice', 'inbox', fresh))).toBe(true);
  });

  it('too-shallow root → WARN, and sweeps 0 (the incident shape, now loud)', async () => {
    // Bus nested one level below the given root.
    seedZombieAndFresh(join(stRoot, 'default', 'smalltalk'), 'example-mac.alice');
    const rc = await cmdGcServe({ root: stRoot, intervalMs: 2000, once: true }, ctx);
    expect(rc).toBe(0);
    expect(stderrBuf).toContain('too shallow');
    expect(stderrBuf).toContain('# gc: swept 0 redundant inbox file(s)');
    // the real (nested) zombie is untouched by a sweep at the wrong root
    expect(
      existsSync(join(stRoot, 'default', 'smalltalk', 'example-mac.alice', 'inbox', twin))
    ).toBe(true);
  });
});

describe('cmdGcCli — dispatch + arg parsing', () => {
  it('no subcommand → usage, returns 1', () => {
    const rc = cmdGcCli([], ctx);
    expect(rc).toBe(1);
    expect(stderrBuf).toContain('usage:');
  });

  it('--help → usage, returns 0', () => {
    const rc = cmdGcCli(['--help'], ctx);
    expect(rc).toBe(0);
    expect(stderrBuf).toContain('gc serve');
  });

  it('unknown subcommand → usage, returns 1', () => {
    const rc = cmdGcCli(['frobnicate'], ctx);
    expect(rc).toBe(1);
    expect(stderrBuf).toContain('unknown subcommand: frobnicate');
  });

  it('serve --once routes through to a real sweep', async () => {
    seedZombieAndFresh(stRoot, 'example-mac.alice');
    const rc = await cmdGcCli(['serve', '--once'], ctx);
    expect(rc).toBe(0);
    expect(existsSync(join(stRoot, 'example-mac.alice', 'inbox', twin))).toBe(false);
  });

  it('serve --root overrides $ST_ROOT', async () => {
    const other = join(scratch, 'other-bus');
    seedZombieAndFresh(other, 'example-mac.bob');
    const rc = await cmdGcCli(['serve', '--root', other, '--once'], ctx);
    expect(rc).toBe(0);
    expect(existsSync(join(other, 'example-mac.bob', 'inbox', twin))).toBe(false);
  });

  it('--interval rejects non-positive / non-numeric', () => {
    expect(() => cmdGcCli(['serve', '--interval', 'abc'], ctx)).toThrowError(
      /--interval/
    );
    expect(() => cmdGcCli(['serve', '--interval', '0'], ctx)).toThrowError(
      /--interval/
    );
    expect(() => cmdGcCli(['serve', '--interval', '-1'], ctx)).toThrowError(
      /--interval/
    );
  });

  it('--interval requires a value', () => {
    expect(() => cmdGcCli(['serve', '--interval'], ctx)).toThrowError(
      /--interval requires a value/
    );
  });

  it('unknown flag → throws', () => {
    expect(() => cmdGcCli(['serve', '--nope'], ctx)).toThrowError(/unknown flag/);
  });
});
