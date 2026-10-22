"""Checks what a build produced, not the runner it ran on.

Every Go binary in the given artifacts must be built with at least the Go
version go.mod's `go` line names, from the xray-core version go.mod requires:
Xray-core v26.10.21's certificate fix depends on both. An Android AAR is
checked through each of its jni/*/libgojni.so. With --no-cxx-runtime, a
Windows DLL must not import a C++ runtime, which a clean Windows 10 lacks.

    python build/verify_core.py linux_so/libXray.so
    python build/verify_core.py --no-cxx-runtime windows_dll/libXray.dll
    python build/verify_core.py libXray.aar
"""
import argparse
import re
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

GO_MOD = Path(__file__).resolve().parent.parent / "go.mod"
XRAY_CORE = "github.com/xtls/xray-core"


def version_tuple(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def version_text(version: tuple[int, ...]) -> str:
    return ".".join(str(part) for part in version)


def parse_go_mod(text: str) -> tuple[tuple[int, ...], str]:
    """The `go` line's version and the xray-core version go.mod requires."""
    go = re.search(r"^go\s+(\d+(?:\.\d+)*)\s*$", text, re.M)
    xray = re.search(rf"^\s*(?:require\s+)?{re.escape(XRAY_CORE)}\s+(\S+)", text, re.M)
    if not go or not xray:
        raise ValueError("go.mod has no go line or no xray-core requirement")
    return version_tuple(go.group(1)), xray.group(1)


def parse_version_m(text: str) -> tuple[tuple[int, ...] | None, str | None]:
    """The Go version and xray-core version `go version -m` reports."""
    first = text.splitlines()[0] if text.strip() else ""
    go = re.search(r":\s+go(\d+(?:\.\d+)*)", first)
    dep = re.search(rf"^\s*dep\s+{re.escape(XRAY_CORE)}\s+(\S+)", text, re.M)
    return (version_tuple(go.group(1)) if go else None, dep.group(1) if dep else None)


def cxx_runtime_imports(dll_names: list[str]) -> list[str]:
    return sorted(name for name in dll_names if name.upper().startswith(("VCRUNTIME", "MSVCP")))


def problems(binary: str, report: str, floor: tuple[int, ...], xray_core: str) -> list[str]:
    go, dep = parse_version_m(report)
    found = []
    if go is None:
        found.append(f"{binary}: no Go build information")
    elif go < floor:
        found.append(f"{binary}: built with go{version_text(go)}, needs go{version_text(floor)} or later")
    if dep != xray_core:
        found.append(f"{binary}: xray-core {dep}, expected {xray_core}")
    return found


def go_binaries(artifact: Path, workdir: Path) -> list[Path]:
    if artifact.suffix != ".aar":
        return [artifact]
    with zipfile.ZipFile(artifact) as aar:
        names = [n for n in aar.namelist() if n.startswith("jni/") and n.endswith("/libgojni.so")]
        for name in names:
            aar.extract(name, workdir)
    if not names:
        raise SystemExit(f"{artifact}: no jni/*/libgojni.so")
    return [workdir / name for name in names]


def dll_imports(path: Path) -> list[str]:
    import pefile  # the Windows leg installs it

    pe = pefile.PE(str(path), fast_load=True)
    pe.parse_data_directories(directories=[pefile.DIRECTORY_ENTRY["IMAGE_DIRECTORY_ENTRY_IMPORT"]])
    return [entry.dll.decode() for entry in getattr(pe, "DIRECTORY_ENTRY_IMPORT", [])]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--no-cxx-runtime", action="store_true")
    parser.add_argument("artifacts", nargs="+", type=Path)
    args = parser.parse_args(argv)
    floor, xray_core = parse_go_mod(GO_MOD.read_text())

    found = []
    with tempfile.TemporaryDirectory() as tmp:
        for artifact in args.artifacts:
            for binary in go_binaries(artifact, Path(tmp)):
                run = subprocess.run(["go", "version", "-m", str(binary)], capture_output=True, text=True)
                report = run.stdout if run.returncode == 0 else ""
                these = problems(str(binary), report, floor, xray_core)
                found += these
                if not these:
                    print(f"{binary}: go>={version_text(floor)}, xray-core {xray_core}")
            if args.no_cxx_runtime and artifact.suffix.lower() == ".dll":
                runtime = cxx_runtime_imports(dll_imports(artifact))
                if runtime:
                    found.append(f"{artifact}: imports a C++ runtime: {', '.join(runtime)}")
    for problem in found:
        print(problem, file=sys.stderr)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
