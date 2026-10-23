import os.path
import subprocess

from app.build import Builder
from app.cmd import create_dir_if_not_exists, delete_dir_if_exists


class MacOSBuilder(Builder):
    """A c-shared libXray.dylib, universal (arm64 + x86_64), for desktop apps
    that drive libXray over plain FFI as they do the Windows DLL and the Linux
    .so — the Apple build makes static xcframeworks only. Same SDK, minimum
    version and flags as the Apple build's macOS slices."""

    # As AppleGoBuilder's macOS targets.
    min_version = "12.0"
    arches = (("arm64", "arm64"), ("amd64", "x86_64"))

    def __init__(
        self,
        build_dir: str,
        use_local_xray_core: bool = False,
        keep_go_env_snapshot: bool = True,
    ):
        super().__init__(build_dir, use_local_xray_core, keep_go_env_snapshot)
        self.framework_dir = os.path.join(self.lib_dir, "macos_dylib")
        delete_dir_if_exists(self.framework_dir)
        create_dir_if_not_exists(self.framework_dir)
        self.lib_file = "libXray.dylib"
        self.lib_header_file = "libXray.h"

    def before_build(self):
        super().before_build()
        self.prepare_static_lib()

    def build(self):
        self.snapshot_go_env()
        try:
            self.before_build()
            self.build_macos()
        finally:
            self.restore_go_env()

    def build_macos(self):
        sdk_path = self.get_sdk_dir_path()
        arch_files = [self.build_arch(go_arch, apple_arch, sdk_path) for go_arch, apple_arch in self.arches]

        universal_file = os.path.join(self.framework_dir, self.lib_file)
        self.run(["lipo", "-create", *arch_files, "-output", universal_file], "lipo -create")
        # Go names the library after its build path; apps load it from their
        # own bundle.
        self.run(["install_name_tool", "-id", f"@rpath/{self.lib_file}", universal_file], "install_name_tool")

        # One header (the same for every arch); the per-arch files go.
        os.replace(
            arch_files[0].replace(".dylib", ".h"),
            os.path.join(self.framework_dir, self.lib_header_file),
        )
        for arch_file in arch_files:
            os.remove(arch_file)
            header = arch_file.replace(".dylib", ".h")
            if os.path.exists(header):
                os.remove(header)

    def build_arch(self, go_arch: str, apple_arch: str, sdk_path: str) -> str:
        output_file = os.path.join(self.framework_dir, f"libXray-{go_arch}.dylib")
        flags = f"-isysroot {sdk_path} -mmacosx-version-min={self.min_version} -arch {apple_arch}"
        run_env = os.environ.copy()
        run_env["GOOS"] = "darwin"
        run_env["GOARCH"] = go_arch
        run_env["GOFLAGS"] = "-tags=darwin"
        run_env["CC"] = "xcrun --sdk macosx --toolchain macosx clang"
        run_env["CXX"] = "xcrun --sdk macosx --toolchain macosx clang++"
        run_env["CGO_CFLAGS"] = flags
        run_env["CGO_CXXFLAGS"] = flags
        run_env["CGO_LDFLAGS"] = flags
        run_env["CGO_ENABLED"] = "1"

        cmd = [
            "go",
            "build",
            "-trimpath",
            "-ldflags",
            "-s -w -buildid=",
            f"-o={output_file}",
            "-buildmode=c-shared",
            self.main_package(),
        ]
        os.chdir(self.lib_dir)
        print(cmd, f"GOARCH={go_arch}")
        ret = subprocess.run(cmd, env=run_env)
        if ret.returncode != 0:
            raise Exception(f"build_macos {go_arch} failed")
        return output_file

    def get_sdk_dir_path(self) -> str:
        ret = subprocess.run(["xcrun", "--sdk", "macosx", "--show-sdk-path"], capture_output=True)
        if ret.returncode != 0:
            raise Exception("get_sdk_dir_path for macosx failed")
        return ret.stdout.decode().strip()

    @staticmethod
    def run(cmd: list[str], what: str):
        print(cmd)
        if subprocess.run(cmd).returncode != 0:
            raise Exception(f"{what} failed")
