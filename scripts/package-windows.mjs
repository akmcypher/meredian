// Builds the Windows package: the committed source tree of a commit plus the launcher from
// installer/windows at its root, as one zip for x64 and ARM64 (the launcher picks the native
// toolchain at install time). Only tracked files are archived, so local state (.env, .meridian/,
// node_modules) can never be included.
// Usage: node scripts/package-windows.mjs <output-dir> <version> [commit=HEAD]
import { execFileSync } from "node:child_process";
import { mkdirSync } from "node:fs";
import { join } from "node:path";

const [out, version, ref = "HEAD"] = process.argv.slice(2);
if (!out || !version || !/^[\w.-]+$/.test(version)) {
  console.error("usage: node scripts/package-windows.mjs <output-dir> <version> [commit]");
  process.exit(2);
}
const git = (...args) => execFileSync("git", args, { encoding: "utf8" }).trim();
const commit = git("rev-parse", "--verify", `${ref}^{commit}`);
const launcher = ["meridian.ps1", "meridian.cmd", "README.txt"].map((f) =>
  join("installer", "windows", f),
);
for (const file of launcher)
  if (git("ls-files", "--error-unmatch", file) === "")
    throw new Error(`${file} is not tracked; commit it before packaging.`);
const tracked = git("ls-tree", "-r", "--name-only", commit).split("\n");
for (const forbidden of [/^\.env$/, /^\.meridian\//, /^node_modules\//, /^\.output\//])
  if (tracked.some((p) => forbidden.test(p)))
    throw new Error(`Refusing to package: ${forbidden} is tracked in ${commit}.`);

const name = `meridian-${version}-windows`;
mkdirSync(out, { recursive: true });
const file = join(out, `${name}.zip`);
// --add-file takes the launcher from the checkout (eol=crlf via .gitattributes) and places it at
// the package root, next to scripts/self-host.mjs.
execFileSync("git", [
  "archive",
  "--format=zip",
  `--prefix=${name}/`,
  ...launcher.map((f) => `--add-file=${f}`),
  "-o",
  file,
  commit,
]);
console.log(file);
