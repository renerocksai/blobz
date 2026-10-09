#!/usr/bin/env python3
"""Install the exact, checksum-pinned compiler for GitHub Actions."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import tarfile
import urllib.request
import zipfile

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--version-file", default=".zig-version")
args = parser.parse_args()
version = (root / args.version_file).read_text().strip()
metadata = json.loads((root / ".github/zig-release.json").read_text())
assert version == metadata["version"] == "0.17.0"
arch = {"x86_64": "x86_64", "AMD64": "x86_64", "arm64": "aarch64", "aarch64": "aarch64"}[platform.machine()]
os_name = {"Linux": "linux", "Darwin": "macos", "Windows": "windows"}[platform.system()]
artifact = metadata["artifacts"][f"{arch}-{os_name}"]
assert artifact["tarball"].startswith("https://ziglang.org/download/0.17.0/")
directory = Path(os.environ["RUNNER_TEMP"]) / "zig-0.17.0"
directory.mkdir()
archive = directory / ("zig.zip" if os_name == "windows" else "zig.tar.xz")
with urllib.request.urlopen(artifact["tarball"], timeout=180) as response:
    archive.write_bytes(response.read())
assert hashlib.sha256(archive.read_bytes()).hexdigest() == artifact["shasum"]
if os_name == "windows":
    with zipfile.ZipFile(archive) as package:
        package.extractall(directory)
else:
    with tarfile.open(archive) as package:
        package.extractall(directory, filter="data")
compiler, = directory.glob("*/zig.exe" if os_name == "windows" else "*/zig")
assert subprocess.check_output([str(compiler), "version"], text=True).strip() == version
with open(os.environ["GITHUB_PATH"], "a", encoding="utf-8") as path:
    path.write(str(compiler.parent) + "\n")
result = {"zig": version, "compiler": str(compiler), "platform": platform.platform(), "machine": platform.machine(), "windows_build": platform.win32_ver(), "runner_image": os.environ.get("ImageVersion"), "archive_sha256": artifact["shasum"], "revision": os.environ.get("GITHUB_SHA")}
packet = Path(os.environ["RUNNER_TEMP"]) / "blobz-ci"
packet.mkdir(exist_ok=True)
(packet / "environment.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result, indent=2))
