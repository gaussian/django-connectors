"""The package must stay cheap to import and free of eager dlt imports.

``import dlt`` measures ~0.6s against ~0.03s for ``import django`` and ~0.02s
for a bare interpreter. That cost is paid by every ``manage.py`` invocation,
every autoreload cycle, every worker fork and every test collection — including
in host applications that never run a pipeline in that process.

These assertions have to run in subprocesses: by the time pytest gets here, the
landing tests have already imported dlt into this interpreter.
"""

import ast
import pathlib
import subprocess
import sys

import pytest

PACKAGE_ROOT = pathlib.Path(__file__).resolve().parent.parent / "django_connectors"
REPO_ROOT = PACKAGE_ROOT.parent

# Modules reachable during Django's app loading. `admin` is eagerly
# autodiscovered whenever django.contrib.admin is installed, and `checks` runs
# on every `manage.py` command, so these may import Django and the stdlib only.
APP_LOADING_MODULES = (
    "__init__.py",
    "apps.py",
    "conf.py",
    "checks.py",
    "errors.py",
    "exceptions.py",
    "registry.py",
    "admin.py",
    "_uuid.py",
)


def _run(code: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )


def test_importing_the_package_does_not_import_dlt():
    result = _run(
        "import sys, django_connectors\n"
        "assert 'dlt' not in sys.modules, 'dlt was imported'\n"
        "print(django_connectors.__version__)\n"
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "0.0.1"


def test_django_setup_does_not_import_dlt():
    result = _run(
        "import os, sys\n"
        "os.environ['DJANGO_SETTINGS_MODULE'] = 'tests.settings'\n"
        "import django\n"
        "django.setup()\n"
        "assert 'dlt' not in sys.modules, 'dlt was imported during django.setup()'\n"
    )
    assert result.returncode == 0, result.stderr


def test_manage_check_opens_no_database_connection():
    result = _run(
        "import os\n"
        "os.environ['DJANGO_SETTINGS_MODULE'] = 'tests.settings'\n"
        "import django\n"
        "django.setup()\n"
        "from django.core.management import call_command\n"
        "from django.db import connections\n"
        "call_command('check')\n"
        "assert connections['default'].connection is None, 'check opened a DB connection'\n"
    )
    assert result.returncode == 0, result.stderr


def _module_level_imports(path: pathlib.Path) -> set[str]:
    """Top-level (module-scope) imported root module names."""
    tree = ast.parse(path.read_text(), filename=str(path))
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names.add(node.module.split(".")[0])
    return names


@pytest.mark.parametrize("relative_path", APP_LOADING_MODULES)
def test_app_loading_modules_import_only_django_and_stdlib(relative_path):
    path = PACKAGE_ROOT / relative_path
    if not path.exists():
        pytest.skip(f"{relative_path} does not exist yet")

    allowed = set(sys.stdlib_module_names) | {"django", "django_connectors"}
    offenders = _module_level_imports(path) - allowed
    assert not offenders, (
        f"{relative_path} imports {sorted(offenders)} at module scope. "
        f"Modules reachable during app loading may import Django and the "
        f"stdlib only — import third-party packages inside functions."
    )


def test_no_module_scope_dlt_import_anywhere_in_the_package():
    offenders = []
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        if "dlt" in _module_level_imports(path):
            offenders.append(path.relative_to(PACKAGE_ROOT).as_posix())
    assert not offenders, (
        f"module-scope `import dlt` in {offenders}; import it inside functions "
        f"so hosts that never run a pipeline never pay for it"
    )
