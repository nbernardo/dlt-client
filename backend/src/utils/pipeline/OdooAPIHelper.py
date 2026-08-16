import dlt
import requests


def odoo_call(url: str, service: str, method: str, args: list):
    resp = requests.post(
        f"{url}/jsonrpc",
        json={
            "jsonrpc": "2.0",
            "method": "call",
            "params": {"service": service, "method": method, "args": args},
            "id": 1,
        },
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(data["error"])
    return data["result"]


def get_uid(url: str, db: str, username: str, password: str) -> int:
    uid = odoo_call(url, "common", "login", [db, username, password])
    if not uid:
        raise RuntimeError("Odoo authentication failed - check credentials")
    return uid


# Field types that exist as real columns on the model's own Postgres table.
# one2many / many2many are relations stored in *other* tables, so a raw
# Postgres SELECT * on this table would never include them.
_COLUMN_TYPES = {
    "char", "text", "html", "integer", "float", "monetary", "boolean",
    "date", "datetime", "selection", "many2one", "binary", "reference",
}


def get_column_fields(url: str, db: str, uid: int, password: str, model: str) -> dict:
    """Return {field_name: field_type} for fields that are actual stored
    Postgres columns on `model` (excludes computed/related, non-stored fields
    like message_*, activity_*, tax_totals__*, display_name, invoice_count...)."""
    fields_meta = odoo_call(
        url,
        "object",
        "execute_kw",
        [db, uid, password, model, "fields_get", [], {"attributes": ["type", "store"]}],
    )
    return {
        name: meta["type"]
        for name, meta in fields_meta.items()
        if meta["type"] in _COLUMN_TYPES and meta.get("store")
    }


def flatten_record(record: dict, field_types: dict) -> dict:
    """Unwrap Odoo's [id, display_name] many2one shape down to a raw id,
    matching what you'd get from a direct Postgres SELECT."""
    flat = {}
    for key, value in record.items():
        if field_types.get(key) == "many2one" and isinstance(value, (list, tuple)):
            flat[key] = value[0] if value else None
        elif value is False:
            # Odoo uses False as its "empty" sentinel for most types over RPC;
            # Postgres would just have NULL.
            flat[key] = None
        else:
            flat[key] = value
    return flat


def search_read_page(
    url: str,
    db: str,
    uid: int,
    password: str,
    model: str,
    domain: list,
    fields: list,
    offset: int,
    limit: int,
):
    kwargs = {"fields": fields, "offset": offset, "limit": limit, "order": "id asc"}
    return odoo_call(
        url,
        "object",
        "execute_kw",
        [db, uid, password, model, "search_read", [domain], kwargs],
    )


def paginated_search_read(
    url: str,
    db: str,
    uid: int,
    password: str,
    model: str,
    domain: list,
    page_size: int = 1000,
):
    """Yields one page (list of flattened rows) at a time instead of loading
    the whole table into memory. `order: id asc` is required here -- without
    an explicit order, Odoo doesn't guarantee stable ordering across pages,
    which can silently skip or duplicate rows as records are written."""
    field_types = get_column_fields(url, db, uid, password, model)
    fields = list(field_types.keys())
    domain = list(domain)
    if "active" in field_types:
        domain.append(("active", "in", [True, False]))

    offset = 0
    while True:
        rows = search_read_page(url, db, uid, password, model, domain, fields, offset, page_size)
        if not rows:
            break
        yield [flatten_record(row, field_types) for row in rows]
        if len(rows) < page_size:
            break
        offset += page_size