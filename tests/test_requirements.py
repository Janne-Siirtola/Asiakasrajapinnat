import ast
import os
import re
import sys
import pytz # Test that pytz is available

PROJECT_DIRS = ["asiakasrajapinnat_master", "config_page"]


def parse_requirements(path="requirements.txt"):
    reqs = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            pkg = re.split(r"[=<>]", line, 1)[0]
            reqs.add(pkg.lower())
    return reqs


def gather_imports():
    modules = set()
    for base in PROJECT_DIRS:
        for root, _, files in os.walk(base):
            for name in files:
                if not name.endswith(".py"):
                    continue
                file_path = os.path.join(root, name)
                with open(file_path, "r", encoding="utf-8") as fh:
                    tree = ast.parse(fh.read(), filename=file_path)
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        for alias in node.names:
                            modules.add(alias.name.split(".")[0])
                    elif isinstance(node, ast.ImportFrom):
                        if node.level == 0 and node.module:
                            modules.add(node.module.split(".")[0])
    return modules


def test_all_required_packages_present():
    requirements = parse_requirements()
    imported = gather_imports()

    stdlib = set(sys.stdlib_module_names)
    internal = {"asiakasrajapinnat_master", "config_page"}

    missing = sorted(
        mod for mod in imported
        if mod not in requirements
        and mod not in stdlib
        and mod not in internal
        and not mod.startswith("azure")
    )

    # Azure packages have names like azure-core, azure-functions etc.
    if "azure" in imported and not any(r.startswith("azure") for r in requirements):
        missing.append("azure")

    assert not missing, f"Missing packages in requirements.txt: {missing}"
