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
```

## What to copy

- `crm/connectors.py` — the only file that talks to the library.
- The `DJANGO_CONNECTORS` block in `example_project/settings.py`.
- `example_project/urls.py` — note webhook URLs are mounted separately from the
  API, because a provider calling back is not an API client.

The example uses the `memory` source so it runs with no credentials. Swapping in
`rest`, `sql` or `filesystem` is a change to `Binding.config` only — no code.
