import { BaseController } from "../../../../@still/component/super/service/BaseController.js";
import { AppTemplate } from "../../../../config/app-template.js";
import jsyaml from "https://cdn.jsdelivr.net/npm/js-yaml@4.1.0/dist/js-yaml.mjs";

/** Small DOM helper: el('div', 'cls', 'text') */
const el = (tag, cls = '', text = '') => {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text !== '') node.textContent = text;
  return node;
};

/**
 * Visual (form) mode of the custom mapping editor.
 *
 * It is only another VIEW over the YAML: the YAML editor stays the single source of truth.
 *  - entering the visual mode parses the YAML and builds one input per leaf property
 *    (label = tag name), nested tags become collapsible groups;
 *  - every change in an input is written back to the YAML editor, which in turn refreshes the
 *    XML output / SQL exactly as if it had been typed in the YAML;
 *  - typing "{" in an input offers the available tables, "{schema.table." offers its columns
 *    (same rules as the YAML intellisense, because it uses the same CustomMappingController data).
 *
 * Used by CustomMappingController:  this.formView = new CustomMappingFormController(this); this.formView.init();
 */
export class CustomMappingFormController extends BaseController {

  /** @type { import('./CustomMappingController.js').CustomMappingController } */ host;

  mode = 'yaml'; // 'yaml' | 'visual'
  tree = null; // object parsed from the YAML while the visual mode is open
  collapsed = new Set(); // ids of the collapsed groups (kept across re-renders)
  dirty = false; // tree changed and not yet written to the YAML editor
  _syncing = false; // true while this controller is writing into the YAML editor
  _seq = 0;

  // suggestions popup state
  items = [];
  activeIndex = 0;
  activeInput = null;

  constructor(host) { super(); this.host = host; }

  /** @returns { HTMLElement } */ $ = (ref) => this.host.$(ref);



  init() {
    const root = this.$('#custom-visual-form');
    if (!root || this.root === root) return;
    this.root = root;

    this.btnYaml = this.$('#cm-mode-yaml');
    this.btnVisual = this.$('#cm-mode-visual');
    this.btnYaml?.addEventListener('click', () => this.setMode('yaml'));
    this.btnVisual?.addEventListener('click', () => this.setMode('visual'));

    // delegated events: the inputs are created dynamically
    root.addEventListener('input', e => this.onInput(e));
    root.addEventListener('keydown', e => this.onKeydown(e));
    root.addEventListener('keyup', e => this.onKeyup(e));
    root.addEventListener('focusout', e => this.onFocusOut(e));
    root.addEventListener('click', e => this.onClick(e));
    root.addEventListener('change', e => this.onLoopChange(e));

    if (!this.popup) {
      this.popup = el('div', 'cmv-popup');
      Object.assign(this.popup.style, {
        position: 'fixed', display: 'none', zIndex: '10000', background: '#fff', border: '1px solid #c9c9c9',
        borderRadius: '4px', boxShadow: '0 4px 14px rgba(0,0,0,.18)', maxHeight: '220px', overflowY: 'auto',
        fontSize: '12px', boxSizing: 'border-box'
      });
      // mousedown (not click) + preventDefault keeps the focus on the input
      this.popup.addEventListener('mousedown', e => {
        e.preventDefault();
        const it = e.target.closest('[data-index]');
        if (it) this.select(Number(it.dataset.index));
      });
      (this.host.obj?.container || document.body).appendChild(this.popup);

      window.addEventListener('scroll', e => { if (!this.popup.contains(e.target)) this.closePopup(); }, true);
      window.addEventListener('resize', () => this.closePopup());
    }

    // The YAML can change from outside (e.g. loadXml): keep the form in sync while it is visible
    const editor = this.host.editor;
    if (editor && this._boundEditor !== editor) {
      this._boundEditor = editor;
      editor.onDidChangeModelContent(() => {
        if (this.mode !== 'visual' || this._syncing) return;
        clearTimeout(this._renderTimer);
        this._renderTimer = setTimeout(() => this.render(), 120);
      });
    }
  }

  /* ------------------------------------------------------------------ */
  /* Mode switch                                                         */
  /* ------------------------------------------------------------------ */

  setMode(mode) {
    if (!this.root || mode === this.mode) return;
    const yamlBox = this.host.editor?.getContainerDomNode?.();

    if (mode === 'visual') {
      const error = this.render(); // the form is still hidden here, so an error box is harmless
      if (error) return AppTemplate.toast.warn('Fix the YAML before using the Visual form: ' + error);
      if (yamlBox) yamlBox.style.display = 'none';
      this.root.style.display = 'block';
    } else {
      this.closePopup();
      this.commitNow(); // flush any pending change into the YAML
      this.root.style.display = 'none';
      if (yamlBox) yamlBox.style.display = '';
      this.host.editor?.layout();
    }

    this.mode = mode;
    this.btnYaml?.classList.toggle('active', mode === 'yaml');
    this.btnVisual?.classList.toggle('active', mode === 'visual');
  }

  /* ------------------------------------------------------------------ */
  /* YAML -> form                                                        */
  /* ------------------------------------------------------------------ */

  /** Placeholders parsed as objects become strings, dates become ISO strings (in place) */
  normalize(node) {
    if (Array.isArray(node)) return node.map(v => this.normalize(v));
    if (node instanceof Date) return node.toISOString();
    if (node && typeof node === 'object') {
      const placeholder = this.host.placeholderOf(node);
      if (placeholder) return placeholder;
      this.uniqueLoopKeys(node);
      for (const k of Object.keys(node)) node[k] = this.normalize(node[k]);
    }
    return node;
  }

  /** Several <loop> at the same level come from the XML as a list under "loop":
   *  give each wrapper its own key (loop, loop_2, ...), keeping the position (in place). */
  uniqueLoopKeys(obj) {
    const isWrapperList = (k, v) => this.host.isLoopKey(k) && Array.isArray(v) && v.length > 0
      && v.every(i => i && typeof i === 'object' && !Array.isArray(i) && Object.keys(i).length > 0);

    const entries = Object.entries(obj);
    if (!entries.some(([k, v]) => isWrapperList(k, v))) return;

    const used = new Set(Object.keys(obj));
    const fresh = () => { let n = 2; while (used.has(`loop_${n}`)) n++; used.add(`loop_${n}`); return `loop_${n}`; };

    const out = [];
    for (const [k, v] of entries) {
      if (isWrapperList(k, v)) v.forEach((item, i) => out.push([i === 0 ? k : fresh(), item]));
      else out.push([k, v]);
    }
    for (const k of Object.keys(obj)) delete obj[k];
    for (const [k, v] of out) obj[k] = v;
  }

  parse() {
    try {
      const data = this.normalize(this.host.yamlToObject(this.host.editor.getValue()));
      if (!data || typeof data !== 'object' || Array.isArray(data))
        return { error: 'The YAML root must be a mapping (key: value)' };
      return { data };
    } catch (err) {
      return { error: String(err?.message || err).split('\n')[0] };
    }
  }

  /** Rebuilds the form from the YAML. Returns an error message, or null when it worked. */
  render() {
    const { data, error } = this.parse();
    const scrollTop = this.root.scrollTop;

    if (error) {
      this.tree = null;
      this.root.replaceChildren(el('div', 'cmv-error', 'Invalid YAML: ' + error));
      return error;
    }

    this.tree = data;
    const frag = document.createDocumentFragment();
    frag.append(el('div', 'cmv-hint', 'Type { in a field to pick a table column. Tick "loop" to wrap a tag in <loop> (repeated once per data row). Every change is written to the YAML.'));
    for (const [key, value] of Object.entries(this.tree)) frag.append(this.build(key, value, [key]));

    this.root.replaceChildren(frag);
    this.root.scrollTop = scrollTop;
    return null;
  }

  /**
   * Builds the node of one tag.
   * `loop` = { parentPath, loopKey } when the tag is the only one inside a <loop> wrapper: it is shown
   * as that tag with its "loop" checkbox ticked, instead of an extra "loop" level.
   */
  build(key, value, path, idx, loop = null) {
    if (!loop && this.isLoopWrapper(key, value)) {
      const [tag, inner] = Object.entries(value)[0];
      return this.build(tag, inner, [...path, tag], idx, { parentPath: path.slice(0, -1), loopKey: key });
    }
    if (Array.isArray(value)) {     // repeated tags: one entry per item
      const frag = document.createDocumentFragment();
      value.forEach((item, i) => frag.append(this.build(key, item, [...path, i], i, loop)));
      return frag;
    }
    if (value && typeof value === 'object' && Object.keys(value).length)
      return this.buildGroup(key, value, path, idx, loop);
    return this.buildRow(key, value, path, idx, loop);
  }

  /** <loop> that wraps exactly one tag */
  isLoopWrapper(key, value) {
    return this.host.isLoopKey(key) && !!value && typeof value === 'object'
      && !Array.isArray(value) && Object.keys(value).length === 1;
  }

  /** A tag can be wrapped when it is a single tag (not one item of a repeated tag) and is not the XML root */
  canWrap(path) {
    if (typeof path[path.length - 1] !== 'string') return false;
    return path.length > 1 || Object.keys(this.tree).length > 1;
  }

  /** The "loop" checkbox of a tag (an empty cell when the tag can't be wrapped, to keep the grid aligned) */
  buildLoopControl(key, path, loop) {
    const cell = el('label', 'cmv-loop');
    let parentPath;
    if (loop) parentPath = loop.parentPath;
    else if (this.canWrap(path)) parentPath = path.slice(0, -1);
    else return el('span', 'cmv-loop');

    const box = el('input', 'cmv-loop-chk');
    box.type = 'checkbox';
    box.checked = !!loop;
    box.dataset.parent = JSON.stringify(parentPath);
    box.dataset.tag = key;
    box.dataset.loopKey = loop ? loop.loopKey : '';

    cell.title = loop ? 'Remove the <loop> around this tag' : 'Wrap this tag in a <loop> (repeated once per data row)';
    cell.append(box, document.createTextNode(' loop'));
    return cell;
  }

  buildGroup(key, value, path, idx, loop) {
    const id = JSON.stringify(path), collapsed = this.collapsed.has(id);

    const group = el('div', 'cmv-group' + (collapsed ? ' cmv-collapsed' : '') + (loop ? ' cmv-looped' : ''));
    const head = el('div', 'cmv-head');
    head.dataset.toggle = id;
    head.append(el('span', 'cmv-caret', collapsed ? '\u25B8' : '\u25BE'), el('span', 'cmv-tag', key));
    if (idx !== undefined) head.append(el('span', 'cmv-idx', `#${idx + 1}`));
    head.append(this.buildLoopControl(key, path, loop));

    const body = el('div', 'cmv-body');
    for (const [k, v] of Object.entries(value)) body.append(this.build(k, v, [...path, k]));

    group.append(head, body);
    return group;
  }

  buildRow(key, value, path, idx, loop) {
    const id = `cmv-in-${++this._seq}`;
    const row = el('div', 'cmv-row' + (loop ? ' cmv-looped' : ''));

    const label = el('label', 'cmv-label', key);
    label.htmlFor = id;
    label.title = path.filter(p => typeof p === 'string').join(' > ');
    if (idx !== undefined) label.append(el('span', 'cmv-idx', `#${idx + 1}`));

    const input = el('input', 'cmv-input');
    input.type = 'text';
    input.id = id;
    input.autocomplete = 'off';
    input.spellcheck = false;
    input.placeholder = 'value or {table.field}';
    input.dataset.path = JSON.stringify(path);
    input.value = (value === null || value === undefined || typeof value === 'object') ? '' : String(value);
    this.markMapped(input);

    row.append(label, input, this.buildLoopControl(key, path, loop));
    return row;
  }

  markMapped(input) {
    input.classList.toggle('cmv-mapped', /\{[^{}]+\}/.test(input.value));
  }

  /* ------------------------------------------------------------------ */
  /* Events                                                              */
  /* ------------------------------------------------------------------ */

  onClick(e) {
    if (e.target.closest?.('.cmv-loop')) return;   // the loop checkbox lives inside the header
    const head = e.target.closest?.('.cmv-head');
    if (!head) return;
    const collapsed = head.parentElement.classList.toggle('cmv-collapsed');
    collapsed ? this.collapsed.add(head.dataset.toggle) : this.collapsed.delete(head.dataset.toggle);
    head.firstChild.textContent = collapsed ? '\u25B8' : '\u25BE';
  }

  /** Ticking "loop" wraps the tag in <loop>, unticking removes the wrapper. The YAML is updated right away. */
  onLoopChange(e) {
    const box = e.target.closest?.('.cmv-loop-chk');
    if (!box || !this.tree) return;

    const parentPath = JSON.parse(box.dataset.parent), tag = box.dataset.tag, loopKey = box.dataset.loopKey;
    const done = box.checked ? this.wrapInLoop(parentPath, tag) : this.unwrapLoop(parentPath, loopKey, tag);
    if (!done) { box.checked = !box.checked; return; }   // refused (a message was shown)

    this.dirty = true;
    this.commitNow();   // the YAML shows the <loop> immediately (and so does the XML output)
    this.render();      // paths changed: rebuild the form
  }

  getAt(path) {
    return path.reduce((node, p) => node[p], this.tree);
  }

  nextLoopKey(obj) {
    let key = 'loop', n = 2;
    while (key in obj) key = `loop_${n++}`;
    return key;
  }

  /** Replaces one entry by other entries, in place and at the same position */
  replaceEntry(obj, key, replacement) {
    const entries = Object.entries(obj);
    for (const k of Object.keys(obj)) delete obj[k];
    for (const [k, v] of entries) {
      if (k === key) replacement.forEach(([nk, nv]) => { obj[nk] = nv; });
      else obj[k] = v;
    }
  }

  /** Keeps the collapsed groups collapsed after their path changed */
  remapCollapsed(from, to) {
    const next = new Set();
    for (const id of this.collapsed) {
      const p = JSON.parse(id);
      const inside = from.every((seg, i) => p[i] === seg);
      next.add(inside ? JSON.stringify([...to, ...p.slice(from.length)]) : id);
    }
    this.collapsed = next;
  }

  wrapInLoop(parentPath, tag) {
    const parent = this.getAt(parentPath), loopKey = this.nextLoopKey(parent);
    this.replaceEntry(parent, tag, [[loopKey, { [tag]: parent[tag] }]]);
    this.remapCollapsed([...parentPath, tag], [...parentPath, loopKey, tag]);
    return true;
  }

  unwrapLoop(parentPath, loopKey, tag) {
    const parent = this.getAt(parentPath), wrapper = parent[loopKey];
    if (tag in parent) {
      AppTemplate.toast.warn(`There is already a tag "${tag}" next to this <loop>. Rename one of them first.`);
      return false;
    }
    this.replaceEntry(parent, loopKey, [[tag, wrapper[tag]]]);
    this.remapCollapsed([...parentPath, loopKey, tag], [...parentPath, tag]);
    return true;
  }

  onInput(e) {
    const input = e.target.closest?.('.cmv-input');
    if (!input) return;
    this.setValueFromInput(input);
    this.updateSuggestions(input);
  }

  onKeydown(e) {
    const input = e.target.closest?.('.cmv-input');
    if (!input) return;

    // Ctrl+Space right after "{" lists everything, like the YAML editor
    if (e.ctrlKey && (e.code === 'Space' || e.key === ' ')) {
      e.preventDefault();
      return this.updateSuggestions(input);
    }

    if (this.popup.style.display !== 'none') {
      switch (e.key) {
        case 'ArrowDown': e.preventDefault(); return this.move(1);
        case 'ArrowUp':   e.preventDefault(); return this.move(-1);
        case 'Enter':
        case 'Tab':       e.preventDefault(); return this.select(this.activeIndex);
        case 'Escape':    e.preventDefault(); return this.closePopup();
      }
    }

    if (e.key === 'Enter') e.preventDefault();   // never submit the surrounding <form>
  }

  onKeyup(e) {
    // moving the caret does not fire "input": refresh the suggestions for the new position
    if (['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(e.key)) {
      const input = e.target.closest?.('.cmv-input');
      if (input) this.updateSuggestions(input);
    }
  }

  onFocusOut(e) {
    if (!e.target.classList?.contains('cmv-input')) return;
    this.closePopup();
    this.commitNow();
  }

  /* ------------------------------------------------------------------ */
  /* Form -> YAML                                                        */
  /* ------------------------------------------------------------------ */

  setAt(path, value) {
    let node = this.tree;
    for (let i = 0; i < path.length - 1; i++) node = node[path[i]];
    node[path[path.length - 1]] = value;
  }

  setValueFromInput(input) {
    if (!this.tree) return;
    this.setAt(JSON.parse(input.dataset.path), input.value);
    this.markMapped(input);
    this.scheduleCommit();
  }

  scheduleCommit() {
    this.dirty = true;
    clearTimeout(this._commitTimer);
    this._commitTimer = setTimeout(() => this.commitNow(), 250);
  }

  /** Same output style as the XML -> YAML conversion: "Key:" for empty values, {table.field} unquoted */
  toYaml(tree) {
    return jsyaml.dump(tree, { indent: 2, lineWidth: -1, noRefs: true })
      .replace(/: ''$/gm, ':')
      .replace(/: null$/gm, ':')
      .replace(/: '(\{[A-Za-z0-9_.-]+\.[A-Za-z0-9_]+\})'$/gm, ': $1');
  }

  /** Writes the pending changes into the YAML editor (keeps its undo history) */
  commitNow() {
    clearTimeout(this._commitTimer);
    const editor = this.host.editor;
    if (!this.dirty || !this.tree || !editor) return;
    this.dirty = false;

    this._syncing = true;
    try {
      editor.pushUndoStop();
      editor.executeEdits('visual-form', [{
        range: editor.getModel().getFullModelRange(), text: this.toYaml(this.tree), forceMoveMarkers: true
      }]);
      editor.pushUndoStop();
    } finally {
      this._syncing = false;
    }
  }

  /* ------------------------------------------------------------------ */
  /* Suggestions ({table} then {table.column}), same rules as the YAML    */
  /* ------------------------------------------------------------------ */

  /** What is being typed after the last "{" before the caret, and what to offer for it */
  context(input) {
    const caret = input.selectionStart ?? input.value.length;
    const before = input.value.slice(0, caret), after = input.value.slice(caret);

    const m = before.match(/\{([A-Za-z0-9_.-]*)$/);
    if (!m) return null;

    const typed = m[1], available = this.host.getAvailableTables();

    // "{schema.table." or "{schema.table.partial" -> columns (the longest matching table name wins)
    const tableKey = this.host.findTablePrefix(typed, available);
    if (tableKey) {
      const partial = typed.slice(tableKey.length + 1);
      if (!/^[A-Za-z0-9_]*$/.test(partial)) return null;

      const closed = after.startsWith('}');   // a "}" already follows the caret
      return {
        items: this.filter(this.host.columnsOf(tableKey), partial)
          .map(c => ({ label: c, kind: 'column', insert: closed ? c : c + '}' })),
        start: caret - partial.length, end: caret, closed
      };
    }

    // "{" or "{partial.table.name" -> tables
    return {
      items: this.filter(available, typed).map(t => ({ label: t, kind: 'table', insert: t + '.' })),
      start: caret - typed.length, end: caret, closed: false
    };
  }

  /** Names starting with the text first, then the ones that only contain it */
  filter(list, text) {
    const low = text.toLowerCase(), starts = [], contains = [];
    for (const name of list) {
      const l = name.toLowerCase();
      if (l.startsWith(low)) starts.push(name);
      else if (l.includes(low)) contains.push(name);
    }
    return [...starts, ...contains];
  }

  updateSuggestions(input) {
    const ctx = this.context(input);
    if (!ctx || !ctx.items.length) return this.closePopup();
    this.openPopup(input, ctx.items);
  }

  select(index) {
    const item = this.items[index], input = this.activeInput;
    if (!item || !input) return;

    const ctx = this.context(input);
    if (!ctx) return this.closePopup();

    const v = input.value;
    input.value = v.slice(0, ctx.start) + item.insert + v.slice(ctx.end);
    const pos = ctx.start + item.insert.length + (item.kind === 'column' && ctx.closed ? 1 : 0);
    input.setSelectionRange(pos, pos);
    this.setValueFromInput(input);

    if (item.kind === 'column') {
      this.closePopup();
      this.commitNow();
      this.host.logSqlQueries();      // same as picking a field in the YAML editor: prints SQL + XML
    } else {
      this.updateSuggestions(input);  // table picked -> show its columns right away
    }
  }

  openPopup(input, items) {
    this.activeInput = input;
    this.items = items.slice(0, 200);
    this.activeIndex = 0;

    this.popup.replaceChildren(...this.items.map((it, i) => {
      const row = el('div', 'cmv-sug');
      row.dataset.index = i;
      Object.assign(row.style, { display: 'flex', justifyContent: 'space-between', gap: '12px', padding: '4px 8px', cursor: 'pointer' });
      const kind = el('span', 'cmv-sug-kind', it.kind);
      kind.style.color = '#888';
      row.append(el('span', 'cmv-sug-label', it.label), kind);
      return row;
    }));

    this.position(input);
    this.popup.style.display = 'block';
    this.popup.scrollTop = 0;
    this.markActive();
  }

  position(input) {
    const r = input.getBoundingClientRect();
    const below = window.innerHeight - r.bottom;
    this.popup.style.left = r.left + 'px';
    this.popup.style.minWidth = Math.max(r.width, 260) + 'px';

    if (below < 200 && r.top > below) { // not enough room below: open upwards
      this.popup.style.top = '';
      this.popup.style.bottom = (window.innerHeight - r.top + 2) + 'px';
    } else {
      this.popup.style.bottom = '';
      this.popup.style.top = (r.bottom + 2) + 'px';
    }
  }

  markActive() {
    [...this.popup.children].forEach((row, i) => {
      row.style.background = i === this.activeIndex ? '#dbeafe' : '';
    });
    this.popup.children[this.activeIndex]?.scrollIntoView?.({ block: 'nearest' });
  }

  move(delta) {
    if (!this.items.length) return;
    this.activeIndex = (this.activeIndex + delta + this.items.length) % this.items.length;
    this.markActive();
  }

  closePopup() {
    if (!this.popup) return;
    this.popup.style.display = 'none';
    this.items = [];
    this.activeInput = null;
  }
}