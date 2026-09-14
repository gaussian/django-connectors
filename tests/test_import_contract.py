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
    # Compared with the package, not with a literal: the literal broke on the
    # first version bump, in every test job, for a change unrelated to imports.
    import django_connectors

    assert result.stdout.strip() == django_connectors.__version__


def test_django_setup_does_not_import_dlt():
    result = _run(
        "import os, sys\n"
        "os.environ['DJANGO_SETTINGS_MODULE'] = 'tests.settings'\n"
        "import django\n"
        "django.setup()\n"
        "assert 'dlt' not in sys.modules, 'dlt was imported during django.setup()'\n"
    )
    assert result.returncode == 0, result.stderr


def test_this_packages_checks_open_no_database_connection():
    """Every check this library registers must run without the database.

    Scoped to our own checks on purpose. A blanket "`manage.py check` opens no
    connection" assertion is not achievable by any app: Django's own
    ``JSONField._check_supported()`` reads ``connection.features
    .supports_json_field``, which on sqlite requires a live connection. So the
    meaningful, testable claim is about the checks we control.
    """
    result = _run(
        "import django\n"
        "from django.conf import settings\n"
        "settings.configure(\n"
        "    SECRET_KEY='x',\n"
        "    DATABASES={'default': {'ENGINE': 'django.db.backends.sqlite3',\n"
        "                           'NAME': '/nonexistent/must-not-be-opened.db'}},\n"
        "    INSTALLED_APPS=['django.contrib.contenttypes', 'django.contrib.auth',\n"
        "                    'django_connectors'],\n"
        "    USE_TZ=True,\n"
        ")\n"
        "django.setup()\n"
        "from django.core.checks.registry import registry\n"
        "from django.db import connections\n"
        "ours = [check for check in registry.get_checks()\n"
        "        if check.__module__.startswith('django_connectors')]\n"
        "assert ours, 'no django_connectors checks are registered'\n"
        "for check in ours:\n"
        "    check(app_configs=None)\n"
        "assert connections['default'].connection is None, (\n"
        "    'a django_connectors check opened a DB connection')\n"
        "print(len(ours))\n"
    )
    assert result.returncode == 0, result.stderr
    assert int(result.stdout.strip()) >= 4


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
