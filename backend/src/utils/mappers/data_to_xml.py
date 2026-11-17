"""
Fills an XML template that contains {schema.table.column} placeholders and <loop> wrappers
with the rows returned by the (joined) query.

Template rules
--------------
* "{table.column}" is replaced by the value of that column. The table is everything
  before the last dot, so schema-qualified names work.
* <loop>...</loop> is NOT written to the output. Its children are repeated once per iteration.
* A loop's iterations come from the tables mapped directly inside it (not inside a nested loop):
    - loop WITH a nested loop  -> one iteration per distinct value-set of its own columns
      (rows stay in the order they arrived, so the query must be ordered by the parent key)
    - innermost loop           -> one iteration per row
* LEFT JOIN rows with no child data (all of the loop's columns are NULL) produce no iteration,
  so a parent without children is written without the child block (skip_empty_loops=True).

uses ElementTree.indent. Standard library.
"""
from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime, time
from typing import Any, Callable, Dict, List, Optional, Sequence

log = logging.getLogger(__name__)

LOOP_TAG = "loop"
_PLACEHOLDER = re.compile(r"\{([A-Za-z0-9_.-]+)\}")

Row = Dict[str, Any]


def _table_of(field: str) -> str:
    """'public.sale_order.id' -> 'public.sale_order'"""
    return field.rsplit(".", 1)[0]


def unescape_template(text: str) -> str:
    """
    A template stored/transported as a single line has its line breaks written as the two
    characters backslash + n (also \\r\\n and \\t, and \\" when the string was escaped as JSON).
    Turns those into real whitespace so it can be parsed.
    """
    return (
        text.replace("\\r\\n", "\n")
        .replace("\\n", "\n")
        .replace("\\r", "\n")
        .replace("\\t", "\t")
        .replace('\\"', '"')
    )


def _hashable(value: Any) -> Any:
    try:
        hash(value)
        return value
    except TypeError:
        return repr(value)


class _Renderer:
    def __init__(self, field_order, formatters, row_keys, skip_empty_loops):
        self.fields: List[str] = list(field_order)
        self.field_set = set(self.fields)
        self.formatters: Dict[str, Callable[[Any], Any]] = formatters or {}
        self.row_keys: Dict[str, List[str]] = row_keys or {}
        self.skip_empty_loops = skip_empty_loops
        self.missing: set = set()

        for table, cols in self.row_keys.items():
            for col in cols:
                if col not in self.field_set:
                    raise ValueError(f"row_keys['{table}'] uses '{col}', which is not in field_order")

    # ---------------------------------------------------------------- values
    def _fmt(self, field: str, value: Any) -> str:
        if value is None:
            return ""
        if field in self.formatters:
            return str(self.formatters[field](value))
        if isinstance(value, (datetime, date, time)):
            return value.isoformat()
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    def _sub(self, text: Optional[str], row: Row) -> Optional[str]:
        if not text or "{" not in text:
            return text

        def repl(m: "re.Match[str]") -> str:
            name = m.group(1)
            if name not in self.field_set:
                self.missing.add(name)
                return m.group(0)
            return self._fmt(name, row.get(name))

        return _PLACEHOLDER.sub(repl, text)

    # ----------------------------------------------------------------- loops
    def _scan(self, loop: ET.Element):
        """Placeholders directly in this loop (not in nested loops) and whether it has nested loops."""
        names, nested = set(), False
        stack = list(loop)
        while stack:
            node = stack.pop()
            if node.tag == LOOP_TAG:
                nested = True
                continue
            for text in [node.text, *node.attrib.values()]:
                names.update(n for n in _PLACEHOLDER.findall(text or "") if n in self.field_set)
            stack.extend(node)
        return names, nested

    def _iterations(self, loop: ET.Element, rows: List[Row]) -> List[List[Row]]:
        names, nested = self._scan(loop)
        tables = {_table_of(n) for n in names}

        if not tables: # only constants and/or nested loops: one iteration
            return [rows] if rows else []

        own_fields = [f for f in self.fields if _table_of(f) in tables]
        key_cols = [c for t in sorted(tables) for c in self.row_keys.get(t, [])]
        check_cols = key_cols or own_fields

        def is_empty(r: Row) -> bool:
            return all(r.get(c) is None for c in check_cols)

        if key_cols or nested: # group rows, keep first-seen order
            group_cols = key_cols or own_fields
            groups: Dict[tuple, List[Row]] = {}
            for r in rows:
                if self.skip_empty_loops and is_empty(r):
                    continue
                groups.setdefault(tuple(_hashable(r.get(c)) for c in group_cols), []).append(r)
            return list(groups.values())

        return [[r] for r in rows if not (self.skip_empty_loops and is_empty(r))]   # innermost: row by row

    # rendering
    def _render_nodes(self, template_parent: ET.Element, out: ET.Element, rows: List[Row]) -> None:
        for node in template_parent:
            if node.tag == LOOP_TAG:
                for iteration_rows in self._iterations(node, rows):
                    self._render_nodes(node, out, iteration_rows)
            else:
                out.append(self._render_element(node, rows))

    def _render_element(self, el: ET.Element, rows: List[Row]) -> ET.Element:
        first = rows[0] if rows else {}
        out = ET.Element(el.tag, {k: self._sub(v, first) for k, v in el.attrib.items()})
        text = (el.text or "").strip()
        if text:
            out.text = self._sub(text, first)
        self._render_nodes(el, out, rows)
        return out

    def render(self, root: ET.Element, rows: List[Row]) -> ET.Element:
        if root.tag == LOOP_TAG:
            raise ValueError("The root element cannot be a <loop>")
        return self._render_element(root, rows)


def render_xml(
    template_xml: str,
    field_order: Sequence[str],
    rows: Sequence[Sequence[Any]],
    *,
    skip_empty_loops: bool = True,
    formatters: Optional[Dict[str, Callable[[Any], Any]]] = None,
    row_keys: Optional[Dict[str, List[str]]] = None,
    indent: str = "  ",
    unescape: Optional[bool] = None,
) -> str:
    """
    template_xml      XML text with {schema.table.column} placeholders and <loop> wrappers.
    field_order       column names in the order of the values of each row.
    rows              the query result (each row is a list in field_order order).
    skip_empty_loops  True: a LEFT JOIN row without child data does not create a repetition.
    formatters        {'schema.table.column': callable(value) -> str}, e.g. to format dates.
                      Default: datetime/date -> ISO 8601, bool -> true/false, None -> empty.
    row_keys          {'schema.table': ['schema.table.id']}: columns that identify one row of that
                      table. Use it when the mapped columns alone can't tell two rows apart
                      (the columns must be present in field_order).
    unescape          The template may arrive with its line breaks written as the characters
                      backslash + n ("<?xml ...?>\\n<Root>\\n  <A/>..."). None (default) detects that
                      (no real line break but a "\\n" sequence) and converts it; True always converts;
                      False never does.
    """
    if unescape is None:
        unescape = "\n" not in template_xml and "\\n" in template_xml
    if unescape:
        template_xml = unescape_template(template_xml)
    template_xml = template_xml.strip().lstrip("\ufeff")

    width = len(field_order)
    for i, row in enumerate(rows):
        if len(row) != width:
            raise ValueError(f"Row {i} has {len(row)} values but field_order has {width}")

    row_dicts: List[Row] = [dict(zip(field_order, row)) for row in rows]

    renderer = _Renderer(field_order, formatters, row_keys, skip_empty_loops)
    root = renderer.render(ET.fromstring(template_xml), row_dicts)

    if renderer.missing:
        log.warning("Placeholders not found in field_order (left as is): %s", sorted(renderer.missing))

    ET.indent(root, space=indent)
    body = ET.tostring(root, encoding="unicode")
    body = body.replace(" />", "/>")            # <Tag/> like the template
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + body


def render_xml_file(template_path: str, output_path: str, field_order, rows, **options) -> str:
    with open(template_path, encoding="utf-8") as f:
        xml = render_xml(f.read(), field_order, rows, **options)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(xml)
    return xml