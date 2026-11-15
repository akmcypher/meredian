// Builds the macOS and Linux packages: the committed source tree of a commit plus the POSIX
// launcher (installer/posix/meridian) and that OS's README.txt at its root, as one tar.gz per OS.
// Each covers x64 and ARM64 (the launcher picks the native toolchain at install time). Only
// tracked files are archived, so local state (.env, .meridian/, node_modules) can never be
// included.
// Usage: node scripts/package-posix.mjs <output-dir> <version> [commit=HEAD] [macos] [linux]
// (both systems when neither is named)
import { execFileSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { gzipSync } from "node:zlib";

const [out, version, ...rest] = process.argv.slice(2);
const isOs = (arg) => arg === "macos" || arg === "linux";
const refs = rest.filter((arg) => !isOs(arg));
const ref = refs[0] ?? "HEAD";
const systems = rest.some(isOs) ? [...new Set(rest.filter(isOs))] : ["macos", "linux"];
if (!out || !version || !/^[\w.-]+$/.test(version) || refs.length > 1) {
  console.error(
    "usage: node scripts/package-posix.mjs <output-dir> <version> [commit] [macos|linux ...]",
  );
  process.exit(2);
}
const git = (...args) => execFileSync("git", args, { encoding: "utf8" }).trim();
const commit = git("rev-parse", "--verify", `${ref}^{commit}`);
const launcher = join("installer", "posix", "meridian");
const tracked = git("ls-tree", "-r", "--name-only", commit).split("\n");
for (const forbidden of [/^\.env$/, /^\.meridian\//, /^node_modules\//, /^\.output\//])
  if (tracked.some((p) => forbidden.test(p)))
    throw new Error(`Refusing to package: ${forbidden} is tracked in ${commit}.`);

// git archive takes an added file's mode from the filesystem, which has no executable bit on
// Windows checkouts. Mark the root launcher executable in the tar header so ./meridian works
// after extraction wherever the package was built.
function makeExecutable(tar, path) {
  const field = (offset, length) =>
    tar
      .subarray(offset, offset + length)
      .toString("latin1")
      .replace(/\0.*$/s, "");
  for (let offset = 0; offset + 512 <= tar.length;) {
    if (tar.subarray(offset, offset + 512).every((b) => b === 0)) break;
    const size = parseInt(field(offset + 124, 12).trim() || "0", 8);
    const prefix = field(offset + 345, 155);
    const name = prefix ? `${prefix}/${field(offset, 100)}` : field(offset, 100);
    if (name === path) {
      tar.write("0000755\0", offset + 100, "latin1");
      tar.fill(0x20, offset + 148, offset + 156);
      let sum = 0;
      for (let i = offset; i < offset + 512; i++) sum += tar[i];
      tar.write(`${sum.toString(8).padStart(6, "0")}\0 `, offset + 148, "latin1");
      return;
    }
    offset += 512 + Math.ceil(size / 512) * 512;
  }
  throw new Error(`${path} is missing from the archive.`);
}

mkdirSync(out, { recursive: true });
for (const os of systems) {
  const readme = join("installer", "posix", os, "README.txt");
  for (const file of [launcher, readme])
    if (!tracked.includes(file.replaceAll("\\", "/")))
      throw new Error(`${file} is not tracked in ${commit}; commit it before packaging.`);
  const name = `meridian-${version}-${os}`;
  // --add-file takes the launcher and README from the checkout and places them at the package
  // root, next to scripts/self-host.mjs.
  const tar = execFileSync(
    "git",
    [
      "archive",
      "--format=tar",
      `--prefix=${name}/`,
      `--add-file=${launcher}`,
      `--add-file=${readme}`,
      commit,
    ],
    { maxBuffer: 1024 * 1024 * 1024 },
  );
  makeExecutable(tar, `${name}/meridian`);
  const file = join(out, `${name}.tar.gz`);
  writeFileSync(file, gzipSync(tar, { level: 9 }));
  console.log(file);
}
