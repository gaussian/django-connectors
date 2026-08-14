# django-connectors example

A minimal host application, showing the whole integration surface in two files:
`crm/models.py` (the host's own models, which the library never sees) and
`crm/connectors.py` (a declared target shape plus a writer).

```bash
cd example
uv run --all-extras python manage.py migrate
uv run --all-extras python manage.py demo
```

Expected output — note `evt-1` updated in place rather than duplicated, and
`evt-2` soft-deleted because the source reported it gone:

```
run 1: succeeded loads=[...]
run 2: succeeded loads=[...]
projection: succeeded seen=2 written=1 deleted=1

host records:
  evt-1  signup   live     {'detail': {'plan': 'enterprise'}, 'source_system': 'demo'}
  evt-2  login    deleted  ...

demo ok
```

`demo ok` is printed only when every step is asserted to have happened: both
Runs succeeded, the ProjectionRun succeeded, and the host rows above are what
landed. Anything else exits non-zero — the command is CI's only end-to-end
gate, and `run_binding` reports a failure as a Run *status* rather than by
raising, so a demo that merely printed it would exit 0 on almost any
regression.

## What to copy

- `crm/connectors.py` — the only file that talks to the library.
- The `DJANGO_CONNECTORS` block in `example_project/settings.py`.
- `example_project/urls.py` — note webhook URLs are mounted separately from the
  API, because a provider calling back is not an API client.

The example uses the `memory` source so it runs with no credentials. Swapping in
`rest`, `sql` or `filesystem` is a change to `Binding.config` only — no code.
