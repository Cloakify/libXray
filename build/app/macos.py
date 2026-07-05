import os.path
import subprocess

from app.build import Builder
from app.cmd import create_dir_if_not_exists, delete_dir_if_exists


class MacOSBuilder(Builder):
    """Builds a c-shared dylib (like the Windows DLL / Linux .so) so desktop
    apps can drive libXray over plain FFI instead of linking the static
    xcframework. Produces a universal (arm64 + x86_64) libXray.dylib."""

    def __init__(self, build_dir: str, use_local_xray_core: bool = False):
        super().__init__(build_dir, use_local_xray_core)
        self.framework_dir = os.path.join(self.lib_dir, "macos_dylib")
        delete_dir_if_exists(self.framework_dir)
        create_dir_if_not_exists(self.framework_dir)
        self.lib_file = "libXray.dylib"
        self.lib_header_file = "libXray.h"

    def before_build(self):
        super().before_build()
        self.prepare_static_lib()

    def build(self):
        self.before_build()
        self.build_macos()
        self.after_build()
        self.revert_go_env()

    def build_macos(self):
        output_dir = self.framework_dir
        create_dir_if_not_exists(output_dir)
        os.chdir(self.lib_dir)

        arch_files = []
        for go_arch, cc_arch in (("arm64", "arm64"), ("amd64", "x86_64")):
            output_file = os.path.join(output_dir, f"libXray-{go_arch}.dylib")
            run_env = os.environ.copy()
            run_env["CGO_ENABLED"] = "1"
            run_env["GOOS"] = "darwin"
            run_env["GOARCH"] = go_arch
            run_env["CC"] = f"clang -arch {cc_arch}"

            cmd = [
                "go",
                "build",
                "-trimpath",
                "-ldflags",
                "-s -w",
                f"-o={output_file}",
                "-buildmode=c-shared",
            ]
            print(cmd, f"GOARCH={go_arch}")
            ret = subprocess.run(cmd, env=run_env)
            if ret.returncode != 0:
                raise Exception(f"build_macos {go_arch} failed")
            arch_files.append(output_file)

        universal_file = os.path.join(output_dir, self.lib_file)
        ret = subprocess.run(["lipo", "-create", *arch_files, "-output", universal_file])
        if ret.returncode != 0:
            raise Exception("lipo create failed")

        # Keep one header (identical across arches), drop per-arch leftovers.
        header_src = arch_files[0].replace(".dylib", ".h")
        os.replace(header_src, os.path.join(output_dir, self.lib_header_file))
        for f in arch_files:
            os.remove(f)
            per_arch_header = f.replace(".dylib", ".h")
            if os.path.exists(per_arch_header):
                os.remove(per_arch_header)

    def after_build(self):
        super().after_build()
        self.reset_files()
