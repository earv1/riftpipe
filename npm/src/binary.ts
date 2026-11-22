// Locating the `riftpipe` binary. Resolution order:
//   1. $RIFTPIPE_BIN — an explicit path always wins.
//   2. A prebuilt binary shipped inside this package (`bin/riftpipe`), the
//      way a published release would carry it (the esbuild model).
//   3. A cargo build in an enclosing checkout (`target/release`, then
//      `target/debug`) — the dev loop: build once, the wrapper finds it.
//   4. Bare `riftpipe`, deferring to $PATH.

import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const exe = process.platform === "win32" ? "riftpipe.exe" : "riftpipe";

/** Walk up from `start` looking for `target/<profile>/riftpipe`. */
function findCargoBuild(start: string): string | undefined {
  let dir = start;
  for (let i = 0; i < 6; i++) {
    for (const profile of ["release", "debug"]) {
      const candidate = join(dir, "target", profile, exe);
      if (existsSync(candidate)) return candidate;
    }
    const parent = dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  return undefined;
}

/** The path (or bare command) used to spawn riftpipe. */
export function riftpipeBinary(): string {
  const env = process.env.RIFTPIPE_BIN;
  if (env) return env;
  const here = dirname(fileURLToPath(import.meta.url));
  const packaged = join(here, "..", "bin", exe);
  if (existsSync(packaged)) return packaged;
  return findCargoBuild(here) ?? findCargoBuild(process.cwd()) ?? "riftpipe";
}
