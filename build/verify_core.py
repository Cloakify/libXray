"""Checks what a build produced, not the runner it ran on.

Every Go binary in the given artifacts must be built with at least the Go
version go.mod's `go` line names, from the xray-core version go.mod requires:
Xray-core v26.10.21's certificate fix depends on both. An Android AAR is
checked through each of its jni/*/libgojni.so. With --no-cxx-runtime, a
Windows DLL must not import a C++ runtime, which a clean Windows 10 lacks.
A universal macOS dylib is checked one architecture at a time.

    python build/verify_core.py linux_so/libXray.so
    python build/verify_core.py macos_dylib/libXray.dylib
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


# Runtimes a clean Windows 10 lacks: MSVC's C++ runtime and MinGW's shared
# runtime libraries.
_FOREIGN_RUNTIMES = ("VCRUNTIME", "MSVCP", "LIBSTDC++", "LIBGCC_S", "LIBWINPTHREAD")


def cxx_runtime_imports(dll_names: list[str]) -> list[str]:
    return sorted(name for name in dll_names if name.upper().startswith(_FOREIGN_RUNTIMES))


def dll_problems(artifact: str, dll_names: list[str]) -> list[str]:
    # A PE with no imports parsed means the parse failed, not a clean DLL.
    if not dll_names:
        return [f"{artifact}: no imports could be read"]
    runtime = cxx_runtime_imports(dll_names)
    return [f"{artifact}: imports a runtime a clean Windows lacks: {', '.join(runtime)}"] if runtime else []


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


def is_universal_macho(head: bytes) -> bool:
    """A universal (fat) Mach-O: one file holding a library per architecture."""
    return head[:4] in (b"\xca\xfe\xba\xbe", b"\xca\xfe\xba\xbf")


def go_binaries(artifact: Path, workdir: Path) -> list[Path]:
    with open(artifact, "rb") as f:
        head = f.read(4)
    if is_universal_macho(head):
        # Every architecture checked on its own, not just the first.
        archs = subprocess.run(["lipo", "-archs", str(artifact)], capture_output=True, text=True, check=True)
        thin = []
        for arch in archs.stdout.split():
            out = workdir / f"{artifact.stem}-{arch}{artifact.suffix}"
            subprocess.run(["lipo", str(artifact), "-thin", arch, "-output", str(out)], check=True)
            thin.append(out)
        if not thin:
            raise SystemExit(f"{artifact}: no architectures")
        return thin
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
                    go, _ = parse_version_m(report)
                    print(f"{binary}: go{version_text(go)} (floor {version_text(floor)}), xray-core {xray_core}")
            if args.no_cxx_runtime and artifact.suffix.lower() == ".dll":
                names = dll_imports(artifact)
                found += dll_problems(str(artifact), names)
                print(f"{artifact}: imports {', '.join(sorted(names))}")
    for problem in found:
        print(problem, file=sys.stderr)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
