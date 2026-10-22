"""Tests for build/verify_core.py's checks.

    python3 build/test_verify_core.py
"""
import unittest

import verify_core

GO_MOD = """module github.com/xtls/libxray

go 1.27.3

require (
	github.com/metacubex/age v0.0.0-20260603010618-28d156b4ea78
	github.com/xtls/xray-core v1.260327.1-0.20261021115930-1b1647cd9ddf
	golang.org/x/mobile v0.0.0-20261011034528-197770444867
)
"""

XRAY_CORE = "v1.260327.1-0.20261021115930-1b1647cd9ddf"


def report(go: str, xray_core: str = XRAY_CORE) -> str:
    return (
        f"linux_so/libXray.so: go{go}\n"
        "\tpath\tgithub.com/xtls/libxray/cgo_bridge\n"
        "\tmod\tgithub.com/xtls/libxray\t(devel)\t\n"
        f"\tdep\tgithub.com/xtls/xray-core\t{xray_core}\th1:abc=\n"
        "\tbuild\t-buildmode=c-shared\n"
    )


class VerifyCoreTest(unittest.TestCase):
    def test_go_mod_names_the_floor_and_the_core(self):
        self.assertEqual(verify_core.parse_go_mod(GO_MOD), ((1, 27, 3), XRAY_CORE))

    def test_a_single_line_require_is_read_too(self):
        text = "module m\n\ngo 1.27.3\n\nrequire github.com/xtls/xray-core v1.2.3\n"
        self.assertEqual(verify_core.parse_go_mod(text), ((1, 27, 3), "v1.2.3"))

    def test_go_mod_without_the_core_is_an_error(self):
        with self.assertRaises(ValueError):
            verify_core.parse_go_mod("module m\n\ngo 1.27.3\n")

    def test_the_floor_and_later_pass(self):
        for go in ("1.27.3", "1.27.10", "1.28", "1.28.1"):
            self.assertEqual(verify_core.problems("x", report(go), (1, 27, 3), XRAY_CORE), [], go)

    def test_older_go_fails_compared_as_numbers(self):
        for go in ("1.27.2", "1.27", "1.26.9"):
            found = verify_core.problems("x", report(go), (1, 27, 3), XRAY_CORE)
            self.assertEqual(len(found), 1, go)
            self.assertIn("needs go1.27.3 or later", found[0])

    def test_another_core_fails(self):
        found = verify_core.problems("x", report("1.27.3", "v1.260327.1-0.20260627131803-45cf2898ab12"), (1, 27, 3), XRAY_CORE)
        self.assertEqual(len(found), 1)
        self.assertIn("expected " + XRAY_CORE, found[0])

    def test_no_build_information_fails(self):
        found = verify_core.problems("x", "", (1, 27, 3), XRAY_CORE)
        self.assertIn("x: no Go build information", found)

    def test_a_cxx_runtime_import_is_found(self):
        names = ["KERNEL32.dll", "VCRUNTIME140.dll", "msvcrt.dll", "msvcp140.dll", "api-ms-win-crt-runtime-l1-1-0.dll"]
        self.assertEqual(verify_core.cxx_runtime_imports(names), ["VCRUNTIME140.dll", "msvcp140.dll"])
        self.assertEqual(verify_core.cxx_runtime_imports(["KERNEL32.dll", "msvcrt.dll"]), [])

    def test_mingw_runtime_libraries_are_found_too(self):
        names = ["KERNEL32.dll", "libwinpthread-1.dll", "libgcc_s_seh-1.dll", "libstdc++-6.dll"]
        self.assertEqual(
            verify_core.cxx_runtime_imports(names), ["libgcc_s_seh-1.dll", "libstdc++-6.dll", "libwinpthread-1.dll"]
        )

    def test_a_dll_with_only_system_imports_passes(self):
        self.assertEqual(verify_core.dll_problems("libXray.dll", ["KERNEL32.dll", "msvcrt.dll", "WS2_32.dll"]), [])

    def test_no_imports_read_is_a_failure_not_a_pass(self):
        self.assertEqual(verify_core.dll_problems("libXray.dll", []), ["libXray.dll: no imports could be read"])


if __name__ == "__main__":
    unittest.main()
