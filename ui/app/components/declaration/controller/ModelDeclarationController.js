import { BaseController } from "../../../../@still/component/super/service/BaseController.js";
import { ModelDeclaration } from "../model/ModelDeclaration.js";

const DEFAULT_YAML = `version: 1
tables:
  - table: public.orders as orders
    dimensions:
      - name: id as order_id
      - name: "id + 3"
      - name: "(id + 3) as id_plus_3"
      - name: customer_id
      - name: status as order_status
      - name: "orders.total_revenue - orders.total_discount"
      - name: "orders.total_revenue / NULLIF(orders.distinct_order_count, 0)"

    measures:
      - name: amount as total_revenue
        agg: SUM
      - name: discount_amount as total_discount
        agg: SUM
      - name: id as distinct_order_count
        agg: COUNT_DISTINCT

  - table: public.customers as customers
    dimensions:
      - id as customer_id
      - full_name as customer_name
      - region_id

  - table: public.regions as regions
    dimensions:
      - id as region_id
      - name as region_name

filters:
  - or:
      - "orders.order_status = 'completed'"
      - "orders.order_status = 'shipped'"
  - "regions.region_name = 'North America'"

relationships:
  - name: orders_to_customers
    from_table: orders
    to_table: customers
    join_type: LEFT
    sql_on: "orders.customer_id = customers.customer_id"

  - name: customers_to_regions
    from_table: customers
    to_table: regions
    join_type: LEFT
    sql_on: "customers.region_id = regions.region_id"
`;

const DEFAULT_SCHEMA = {
  'public.orders': ['id', 'customer_id', 'amount', 'discount_amount', 'status', 'created_at','naka_order','naka_name'],
  'public.customers': ['id', 'full_name', 'email', 'region_id'],
  'public.regions': ['id', 'name','region'],
  'public.de_luta': ['eu', 'ele','novoutros'],
};

// Value suggestions for known scalar fields.
const AGG_FUNCTIONS = ['SUM', 'AVG', 'COUNT', 'COUNT_DISTINCT', 'MIN', 'MAX'];
const JOIN_TYPES = ['LEFT', 'RIGHT', 'INNER', 'CROSS', 'FULL'];

// Valid keys per structural context, used for key-name IntelliSense.
// Each entry maps to an array of { key, insertText, detail }.
const KEY_SUGGESTIONS = {
  root: [
    { key: 'version', insertText: 'version: 1', detail: 'Model version' },
    { key: 'tables', insertText: 'tables:\n  - table: ', detail: 'Table declarations' },
    { key: 'filters', insertText: 'filters:\n  - ', detail: 'Global filter conditions' },
    { key: 'relationships', insertText: 'relationships:\n  - name: ', detail: 'Table join declarations' },
  ],
  tableItem: [
    { key: 'table', insertText: 'table: ', detail: 'Real table name, e.g. public.orders as orders' },
    { key: 'dimensions', insertText: 'dimensions:\n - name:', detail: 'Groupable columns/expressions' },
    { key: 'measures', insertText: 'measures:\n. - name:', detail: 'Aggregated/calculated fields' },
  ],
  dimensionItem: [
    { key: 'name', insertText: 'name: ', detail: 'Column, expression, or "expr as alias"' },
    { key: 'sql', insertText: 'sql: "', detail: 'Explicit SQL expression' },
  ],
  measureItem: [
    { key: 'name', insertText: 'name: ', detail: 'Column, or "column as alias"' },
    { key: 'sql', insertText: 'sql: "', detail: 'Calculated/composed measure expression' },
    { key: 'agg', insertText: 'agg: ', detail: 'Aggregation function (SUM, AVG, COUNT, ...)' },
  ],
  relationshipItem: [
    { key: 'name', insertText: 'name: ', detail: 'Relationship identifier' },
    { key: 'from_table', insertText: 'from_table: ', detail: 'Source table alias' },
    { key: 'to_table', insertText: 'to_table: ', detail: 'Target table alias' },
    { key: 'join_type', insertText: 'join_type: ', detail: 'LEFT, RIGHT, INNER, CROSS, FULL' },
    { key: 'sql_on', insertText: 'sql_on: "', detail: 'Join condition, e.g. a.id = b.a_id' },
  ],
  filtersBlockKey: [
    { key: 'or', insertText: 'or:\n      - ', detail: 'OR-combined sub-conditions' },
    { key: 'and', insertText: 'and:\n      - ', detail: 'AND-combined sub-conditions' },
  ],
};

const modelInstanceRegistry = new Map();
let yamlCompletionProviderRegistered = false;

function ensureYamlCompletionProviderRegistered() {
  if (yamlCompletionProviderRegistered) return;
  yamlCompletionProviderRegistered = true;

  monaco.languages.registerCompletionItemProvider('yaml', {
    triggerCharacters: ['.', ' ', ':'],
    provideCompletionItems(model, position) {
      const instance = modelInstanceRegistry.get(model.uri.toString());
      if (!instance) return { suggestions: [] };
      return instance.provideYamlCompletions(model, position);
    }
  });
}

export class ModelDeclarationController extends BaseController {

  /** @type { ModelDeclaration } */ obj;
  editor;
  schema;

  async initEditor() {
    this.obj.container = document.getElementsByClassName(this.obj.cmpInternalId)[0];

    if (this.obj.container) {
      this.obj.yamlInput = this.obj.container.querySelector('#yaml-input');
      this.obj.sqlOutput = this.obj.container.querySelector('#sql-output');
      this.obj.errorBox = this.obj.container.querySelector('#error-box');
    }

    this.editor = monaco.editor.create(this.obj.yamlInput, {
      value: DEFAULT_YAML, language: 'yaml', theme: 'vs-light', automaticLayout: true,
      minimap: { enabled: false }, scrollBeyondLastLine: false,
      fontSize: 14, quickSuggestions: { other: true, comments: false, strings: true }
    });

    this.schema = await this.loadSchema();

    modelInstanceRegistry.set(this.editor.getModel().uri.toString(), this);
    ensureYamlCompletionProviderRegistered();

    this.bindEvents(), this.compileYAMLToSQL();
  }

  async loadSchema() {
    return DEFAULT_SCHEMA;
  }

  destroy() {
    if (this.editor) {
      modelInstanceRegistry.delete(this.editor.getModel().uri.toString());
      this.editor.dispose();
    }
  }

  resetDeclaration = (e) => { e.preventDefault(); this.editor.setValue(DEFAULT_YAML); };
  copySQL = (e) => { e.preventDefault(); this.copySQL(); };

  bindEvents() {
    if (this.editor) this.editor.onDidChangeModelContent(() => this.compileYAMLToSQL());
  }

  cleanValue(val) {
    if (!val) return '';
    val = val.trim();
    if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) 
      val = val.slice(1, -1).trim();
    return val;
  }

  sanitizeAlias(rawStr) {
    if (!rawStr) return 'col';
    const reg1 = /[^a-z0-9_]+/g, reg2 = /^_+|_+$/g
    return rawStr.trim().toLowerCase().replace(reg1, '_').replace(reg2, '') || 'col';
  }

  validateNoTrailingColon(rawStr, contextName) {
    if (!rawStr || typeof rawStr !== 'string') return;
    const trimmed = rawStr.trim();
    if (trimmed.endsWith(':')) 
      throw new Error(`Syntax Error in ${contextName}: Unexpected trailing colon in <code>"${trimmed}"</code>`);
  }

  parseAliasString(str, contextName) {
    if (!str || typeof str !== 'string') return { sql: '', alias: '' };
    
    this.validateNoTrailingColon(str, contextName);
    const cleanStr = this.cleanValue(str);
    
    const parts = cleanStr.split(/\s+as\s+|\s+AS\s+/);
    if (parts.length > 1) {
      const rawSql = this.cleanValue(parts[0]), rawAlias = this.cleanValue(parts[1]);
      this.validateNoTrailingColon(rawAlias, contextName);
      return { sql: rawSql, alias: this.sanitizeAlias(rawAlias) };
    }
    
    return { sql: cleanStr, alias: this.sanitizeAlias(cleanStr) };
  }

  qualifyDimensionExpr(expr, tableAlias) {
    const trimmed = expr.trim();
    
    if (/^[a-zA-Z0-9_]+$/.test(trimmed)) 
      return `${tableAlias}.${trimmed}`;
    
    if (/^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$/.test(trimmed)) 
      return trimmed;

    const sqlKeywords = new Set(['AND', 'OR', 'NOT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'NULL', 'DATE_TRUNC', 'CAST', 'AS', 'COALESCE', 'NULLIF', 'DAY', 'MONTH', 'YEAR']);

    return trimmed.replace(/\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g, (match, offset, string) => {
      if (sqlKeywords.has(match.toUpperCase()) || !isNaN(match)) return match;
      const prevChar = string[offset - 1];
      if (prevChar === '.') return match;
      return `${tableAlias}.${match}`;
    });
  }

  buildAliasMap(yamlText) {
    const map = {}, lines = yamlText.split('\n');
    for (const line of lines) {
      const m = line.trim().match(/^-\s*table:\s*(.+)$/);
      if (!m) continue;
      try {
        const parsed = this.parseAliasString(m[1], 'intellisense-lookup');
        if (parsed.sql) map[parsed.alias] = this.cleanValue(m[1].split(/\s+as\s+|\s+AS\s+/)[0]) || parsed.sql;
      } catch (e) { /* ignore malformed/incomplete lines while typing */ }
    }
    return map;
  }

  findEnclosingTableAlias(model, position) {
    const currentIndent = model.getLineContent(position.lineNumber).search(/\S/);
    const clampedIndent = currentIndent === -1 ? 0 : currentIndent;

    for (let ln = position.lineNumber - 1; ln >= 1; ln--) {
      const raw = model.getLineContent(ln);
      if (!raw.trim()) continue;

      const indent = raw.search(/\S/), trimmed = raw.trim();

      const tableMatch = trimmed.match(/^-\s*table:\s*(.+)$/);
      if (tableMatch && indent <= clampedIndent) {
        try {
          return this.parseAliasString(tableMatch[1], 'intellisense-lookup').alias;
        } catch (e) { return null; }
      }

      if (indent === 0 && /^[a-zA-Z_]+:/.test(trimmed)) break;
    }
    return null;
  }

  suggestAliasFor(tableName) {
    const parts = tableName.split('.');
    return this.sanitizeAlias(parts[parts.length - 1]);
  }


  getStructuralPath(model, position) {
    const cursorLineRaw = model.getLineContent(position.lineNumber);
    const cursorIndent = cursorLineRaw.substring(0, position.column - 1).search(/\S/);
    const effectiveCursorIndent = cursorIndent === -1
      ? cursorLineRaw.substring(0, position.column - 1).length
      : cursorIndent;

    let stack = [{ indent: -1, key: null }];

    for (let ln = 1; ln < position.lineNumber; ln++) {
      const rawLine = model.getLineContent(ln);
      if (!rawLine.trim() || rawLine.trim().startsWith('#')) continue;

      const indent = rawLine.search(/\S/);
      const line = rawLine.trim();

      while (stack.length > 1 && stack[stack.length - 1].indent >= indent) stack.pop();

      if (line.startsWith('- ')) {
        const val = line.substring(2).trim();
        const firstColonIdx = val.indexOf(':');

        if (firstColonIdx !== -1 && !val.startsWith('"') && !val.startsWith("'")) {
          const k = val.substring(0, firstColonIdx).trim();
          const v = val.substring(firstColonIdx + 1).trim();

          stack.push({ indent, key: '@item', itemFirstKey: k });
          if (v === '') stack.push({ indent: indent + 1, key: k });
        } else {
          stack.push({ indent, key: '@item', itemFirstKey: null, bare: true });
        }
      } else if (line.includes(':')) {
        const firstColonIdx = line.indexOf(':');
        const key = line.substring(0, firstColonIdx).trim();
        const val = line.substring(firstColonIdx + 1).trim();

        if (val === '' || val === ':') stack.push({ indent, key });
      }
    }

    while (stack.length > 1 && stack[stack.length - 1].indent >= effectiveCursorIndent) stack.pop();

    return stack;
  }


  resolveKeySuggestions(path) {
    const top = path[path.length - 1];
    const parent = path[path.length - 2];

    if (!top || top.key === null) return { entries: KEY_SUGGESTIONS.root, listStart: false };

    if (top.key === '@item' && parent) {
      const alreadyUsed = top.itemFirstKey;
      const filterUsed = (entries) => entries.filter(e => e.key !== alreadyUsed);

      if (parent.key === 'tables') return { entries: filterUsed(KEY_SUGGESTIONS.tableItem), listStart: false };
      if (parent.key === 'dimensions') return { entries: filterUsed(KEY_SUGGESTIONS.dimensionItem), listStart: false };
      if (parent.key === 'measures') return { entries: filterUsed(KEY_SUGGESTIONS.measureItem), listStart: false };
      if (parent.key === 'relationships') return { entries: filterUsed(KEY_SUGGESTIONS.relationshipItem), listStart: false };
      return null;
    }

    if (top.key === 'tables') {
      return { entries: [{ key: 'table', insertText: 'table: ', detail: 'Real table name, e.g. public.orders as orders' }], listStart: true };
    }
    if (top.key === 'relationships') {
      return { entries: [{ key: 'name', insertText: 'name: ', detail: 'Relationship identifier' }], listStart: true };
    }
    if (top.key === 'filters' || top.key === 'or' || top.key === 'and') {
      return { entries: KEY_SUGGESTIONS.filtersBlockKey, listStart: true };
    }

    return null;
  }

  provideYamlCompletions(model, position) {
    if (!this.schema) return { suggestions: [] };

    const lineContent = model.getLineContent(position.lineNumber);
    const textBeforeCursor = lineContent.substring(0, position.column - 1);

    const tableKeyMatch = textBeforeCursor.match(/^(\s*(?:-\s*)?table:\s*)([a-zA-Z0-9_.]*)$/);
    if (tableKeyMatch) {
      const [, prefix, partial] = tableKeyMatch;
      const lineNum = position.lineNumber, length = prefix.length, kind = monaco.languages.CompletionItemKind.Struct;
      const range = new monaco.Range(lineNum, length + 1, lineNum, length + 1 + partial.length);
      return {
        suggestions: Object.keys(this.schema).map(tableName => ({
          range, label: tableName, detail: 'Table', kind, insertText: `${tableName} as ${this.suggestAliasFor(tableName)}`,
        }))
      };
    }

    // agg: <value> -> suggest aggregation functions
    const aggMatch = textBeforeCursor.match(/^(\s*agg:\s*)([a-zA-Z_]*)$/i);
    if (aggMatch) {
      const [, prefix, partial] = aggMatch;
      const lineNum = position.lineNumber, kind = monaco.languages.CompletionItemKind.EnumMember;
      const range = new monaco.Range(lineNum, prefix.length + 1, lineNum, prefix.length + 1 + partial.length);
      return {
        suggestions: AGG_FUNCTIONS.map(fn => ({ range, label: fn, insertText: fn, kind, detail: 'Aggregation function' }))
      };
    }

    // join_type: <value> -> suggest join types
    const joinTypeMatch = textBeforeCursor.match(/^(\s*join_type:\s*)([a-zA-Z_]*)$/i);
    if (joinTypeMatch) {
      const [, prefix, partial] = joinTypeMatch;
      const lineNum = position.lineNumber, kind = monaco.languages.CompletionItemKind.EnumMember;
      const range = new monaco.Range(lineNum, prefix.length + 1, lineNum, prefix.length + 1 + partial.length);
      return {
        suggestions: JOIN_TYPES.map(jt => ({ range, label: jt, insertText: jt, kind, detail: 'Join type' }))
      };
    }

    // from_table:/to_table: <value> -> suggest table aliases already declared in this document
    const fromToTableMatch = textBeforeCursor.match(/^(\s*(?:from_table|to_table):\s*)([a-zA-Z0-9_]*)$/);
    if (fromToTableMatch) {
      const [, prefix, partial] = fromToTableMatch;
      const aliasMap = this.buildAliasMap(model.getValue());
      const aliases = Object.keys(aliasMap);
      if (aliases.length === 0) return { suggestions: [] };

      const lineNum = position.lineNumber, kind = monaco.languages.CompletionItemKind.Variable;
      const range = new monaco.Range(lineNum, prefix.length + 1, lineNum, prefix.length + 1 + partial.length);
      return {
        suggestions: aliases.map(alias => ({ range, label: alias, insertText: alias, kind, detail: aliasMap[alias] }))
      };
    }

    const kind = monaco.languages.CompletionItemKind.Field;
    const dotMatch = textBeforeCursor.match(/([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z0-9_]*)$/);
    if (dotMatch) {
      const [, alias, partialField] = dotMatch;
      const aliasMap = this.buildAliasMap(model.getValue());
      const realTable = aliasMap[alias];
      const columns = realTable ? this.schema[realTable] : null;

      if (!columns) return { suggestions: [] };
      
      const lineNum = position.lineNumber, col = position.column;
      const range = new monaco.Range(lineNum, col - partialField.length, lineNum, col);
      return { suggestions: columns.map(col => ({ range, label: col, insertText: col, kind, detail: `${realTable}.${col}`, })) };
    }

    const doubleQuoteCount = (textBeforeCursor.match(/"/g) || []).length;
    const singleQuoteCount = (textBeforeCursor.match(/'/g) || []).length;
    const insideOpenDoubleQuote = doubleQuoteCount % 2 === 1;
    const insideOpenSingleQuote = singleQuoteCount % 2 === 1;
    const bareAliasMatch = textBeforeCursor.match(/([a-zA-Z_][a-zA-Z0-9_]*)$/);
    if (insideOpenDoubleQuote && !insideOpenSingleQuote && bareAliasMatch) {
      const [, partial] = bareAliasMatch;
      const aliasMap = this.buildAliasMap(model.getValue());
      const aliases = Object.keys(aliasMap);

      if (aliases.length > 0) {
        const lineNum = position.lineNumber, col = position.column;
        const range = new monaco.Range(lineNum, col - partial.length, lineNum, col);
        return {
          suggestions: aliases.map(alias => ({
            range, label: alias, insertText: `${alias}.`, kind: monaco.languages.CompletionItemKind.Struct,
            detail: aliasMap[alias],
            command: { id: 'editor.action.triggerSuggest', title: 'Suggest columns' }
          }))
        };
      }
    }

    const bareListMatch = textBeforeCursor.match(/^\s*-\s*(?:name:\s*)?([a-zA-Z0-9_]*)$/);
    if (bareListMatch) {
      const [, partial] = bareListMatch;
      const enclosingAlias = this.findEnclosingTableAlias(model, position);
      const aliasMap = this.buildAliasMap(model.getValue());
      const realTable = enclosingAlias ? aliasMap[enclosingAlias] : null;
      const columns = realTable ? this.schema[realTable] : null;

      if (columns) {
        const lineNum = position.lineNumber, col = position.column;
        const range = new monaco.Range(lineNum, col - partial.length, lineNum, col);
        return { suggestions: columns.map(col => ({ range, label: col, insertText: col, kind, detail: `Column on ${enclosingAlias}`, })) };
      }
    }

    const structuralPath = this.getStructuralPath(model, position);
    const keySuggestions = this.resolveKeySuggestions(structuralPath);
    if (keySuggestions) {
      const kindProp = monaco.languages.CompletionItemKind.Property;
      const lineNum = position.lineNumber;

      if (keySuggestions.listStart) {
        const dashMatch = textBeforeCursor.match(/^(\s*-\s*)([a-zA-Z_]*)$/);
        const freshMatch = textBeforeCursor.match(/^(\s*)([a-zA-Z_]*)$/);

        if (dashMatch) {
          const [, prefix, partial] = dashMatch;
          const range = new monaco.Range(lineNum, prefix.length + 1, lineNum, prefix.length + 1 + partial.length);
          const suggestions = keySuggestions.entries.map(e => ({ range, label: e.key, insertText: e.insertText, kind: kindProp, detail: e.detail }))
          return { suggestions };
        }
        if (freshMatch) {
          const [, prefix, partial] = freshMatch;
          const range = new monaco.Range(lineNum, prefix.length + 1, lineNum, prefix.length + 1 + partial.length);
          const suggestions = keySuggestions.entries.map(e => ({ range, label: e.key, insertText: `- ${e.insertText}`, kind: kindProp, detail: e.detail }))
          return { suggestions };
        }
      } else {
        const freshMatch = textBeforeCursor.match(/^(\s*)([a-zA-Z_]*)$/);
        if (freshMatch) {
          const [, prefix, partial] = freshMatch;
          const range = new monaco.Range(lineNum, prefix.length + 1, lineNum, prefix.length + 1 + partial.length);
          const suggestions = keySuggestions.entries.map(e => ({ range, label: e.key, insertText: e.insertText, kind: kindProp, detail: e.detail }))
          return { suggestions };
        }
      }
    }
    return { suggestions: [] };
  }

  parseSimpleYAML(yamlText) {
    let lines = yamlText.split('\n'), root = {};
    let stack = [{ indent: -1, obj: root }];

    for (let i = 0; i < lines.length; i++) {
      let rawLine = lines[i];
      if (!rawLine.trim() || rawLine.trim().startsWith('#')) continue;

      const indent = rawLine.search(/\S/), line = rawLine.trim();

      while (stack.length > 1 && stack[stack.length - 1].indent >= indent) {
        stack.pop();
      }

      let parent = stack[stack.length - 1].obj;

      if (line.startsWith('- ')) {
        const val = line.substring(2).trim();
        const firstColonIdx = val.indexOf(':');

        if (firstColonIdx !== -1 && !val.startsWith('"') && !val.startsWith("'")) {
          const itemObj = {};
          if (Array.isArray(parent)) parent.push(itemObj);

          const k = val.substring(0, firstColonIdx).trim(), v = val.substring(firstColonIdx + 1).trim();

          if (v !== '') {
            itemObj[k] = this.cleanValue(v);
            stack.push({ indent: indent, obj: itemObj });
          } else {
            let nextIsArray = false;
            for (let j = i + 1; j < lines.length; j++) {
              const nextTrim = lines[j].trim();
              if (nextTrim && !nextTrim.startsWith('#')) {
                if (nextTrim.startsWith('-')) nextIsArray = true;
                break;
              }
            }
            itemObj[k] = nextIsArray ? [] : {};
            stack = [...stack, { indent: indent, obj: itemObj }, { indent: indent + 1, obj: itemObj[k] }];
          }
        } else {
          if (Array.isArray(parent)) 
            parent.push(this.cleanValue(val));
        }
      } else if (line.includes(':')) {
        const firstColonIdx = line.indexOf(':');
        const key = line.substring(0, firstColonIdx).trim(), val = line.substring(firstColonIdx + 1).trim();

        if (val === '' || val === ':') {
          let nextIsArray = false;
          for (let j = i + 1; j < lines.length; j++) {
            const nextTrim = lines[j].trim();
            if (nextTrim && !nextTrim.startsWith('#')) {
              if (nextTrim.startsWith('-')) nextIsArray = true;
              break;
            }
          }
          parent[key] = nextIsArray ? [] : {};
          stack.push({ indent: indent, obj: parent[key] });
        } else 
          parent[key] = this.cleanValue(val);
      }
    }
    return root;
  }

  validateFilterSyntax(filterExpr) {
    if (!filterExpr || typeof filterExpr !== 'string') return;
    const trimmed = filterExpr.trim();

    const singleQuotes = (trimmed.match(/'/g) || []).length;
    if (singleQuotes % 2 !== 0) {
      throw new Error(`Syntax Error in filter: Unclosed single quote (') in expression <code>"${trimmed}"</code>`);
    }

    const doubleQuotes = (trimmed.match(/"/g) || []).length;
    if (doubleQuotes % 2 !== 0) {
      throw new Error(`Syntax Error in filter: Unclosed double quote (") in expression <code>"${trimmed}"</code>`);
    }

    if (/(=|>|<|!=|LIKE|AND|OR)\s*$/i.test(trimmed) || /=\s*['"]?\s*$/.test(trimmed)) {
      throw new Error(`Syntax Error in filter: Incomplete expression or missing operand in <code>"${trimmed}"</code>`);
    }
  }

  buildBaseMeasureSQL(tableAlias, sqlCol, aggType) {
    const aggUpper = (aggType || 'SUM').toUpperCase().replace('-', '_');
    if (aggUpper === 'COUNT_DISTINCT') {
      return `COUNT(DISTINCT ${tableAlias}.${sqlCol})`;
    }
    if (['SUM', 'COUNT', 'AVG', 'MIN', 'MAX'].includes(aggUpper)) {
      return `${aggUpper}(${tableAlias}.${sqlCol})`;
    }
    return `${sqlCol}`;
  }

  compileYAMLToSQL() {
    if (this.obj.errorBox) this.obj.errorBox.style.display = 'none';

    try {
      if (!this.editor) return;
      const yamlText = this.editor.getValue();
      const model = this.parseSimpleYAML(yamlText);

      if (!model.tables || model.tables.length === 0) {
        if (this.obj.sqlOutput) this.obj.sqlOutput.textContent = '-- Define at least one table in YAML to compile SQL.';
        return;
      }

      const normalizedTables = (model.tables || []).map((t, idx) => {
        this.validateNoTrailingColon(t.table, `table definition #${idx + 1}`);
        const parsedTable = this.parseAliasString(t.table, `table definition '${t.table}'`);

        const dimensions = (t.dimensions || []).map(d => {
          let rawStr = typeof d === 'string' ? d : (d.name || d.sql || d.column || '');
          const parsed = this.parseAliasString(rawStr, `dimension in '${parsedTable.alias}'`);
          return { alias: parsed.alias, sqlColumn: parsed.sql };
        });

        const baseMeasures = [], calculatedMeasures = [];

        (t.measures || []).forEach(m => {
          let rawStr = typeof m === 'string' ? m : (m.name || m.sql || '');
          let agg = typeof m === 'object' ? this.cleanValue(m.agg || '') : '';
          let explicitSql = typeof m === 'object' ? this.cleanValue(m.sql || '') : '';

          const parsed = this.parseAliasString(rawStr, `measure in '${parsedTable.alias}'`);

          const isCalculated = explicitSql || agg.toUpperCase() === 'CALCULATED' || 
            parsed.sql.includes('/') || parsed.sql.includes('*') || parsed.sql.includes('+') || parsed.sql.includes('-');

          if (isCalculated) 
            calculatedMeasures.push({ alias: parsed.alias || parsed.sql, formula: explicitSql || parsed.sql });
          else 
            baseMeasures.push({ alias: parsed.alias, sqlColumn: parsed.sql, agg: agg || 'SUM' });
        });

        return { alias: parsedTable.alias, sqlTable: parsedTable.sql, dimensions, baseMeasures, calculatedMeasures };
      });

      const baseTableObj = normalizedTables[0];
      const baseTableAlias = baseTableObj.alias;

      const validateFieldRef = (tAlias, fName, contextLocation) => {
        const tableDef = normalizedTables.find(t => t.alias === tAlias);
        if (!tableDef) {
          const avail = normalizedTables.map(t => t.alias).join(', ');
          throw new Error(`Table alias '${tAlias}' referenced in ${contextLocation} does not exist. Available tables: [${avail}]`);
        }

        const dim = tableDef.dimensions.find(d => d.alias === fName || d.sqlColumn === fName);
        const meas = tableDef.baseMeasures.find(m => m.alias === fName || m.sqlColumn === fName);
        const calcMeas = tableDef.calculatedMeasures.find(m => m.alias === fName);

        if (!dim && !meas && !calcMeas) {
          const availDims = tableDef.dimensions.flatMap(d => [d.alias, d.sqlColumn]);
          const availMeas = tableDef.baseMeasures.flatMap(m => [m.alias, m.sqlColumn]);
          const availCalc = tableDef.calculatedMeasures.map(m => m.alias);
          const allFields = Array.from(new Set([...availDims, ...availMeas, ...availCalc])).filter(Boolean).join(', ');
          throw new Error(
            `Field '${fName}' referenced in ${contextLocation} is not defined on table '${tAlias}'.<br>` +
            `Available fields on '${tAlias}': [<b>${allFields || 'none'}</b>]`
          );
        }
      };

      const validateSqlExpression = (sqlExpr, contextLocation) => {
        const matches = sqlExpr.matchAll(/([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)/g);
        for (const match of matches) {
          const [_, tAlias, fName] = match;
          if (normalizedTables.some(t => t.alias === tAlias)) 
            validateFieldRef(tAlias, fName, contextLocation);
        }
      };

      const expandCalculatedFormula = (rawFormula) => {
        let expanded = rawFormula, matches = [...rawFormula.matchAll(/([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)/g)];

        for (const match of matches) {
          const [fullRef, tAlias, fName] = match;
          const targetTable = normalizedTables.find(t => t.alias === tAlias);

          if (targetTable) {
            const baseMeas = targetTable.baseMeasures.find(m => m.alias === fName || m.sqlColumn === fName);
            if (baseMeas) {
              const baseExpr = this.buildBaseMeasureSQL(tAlias, baseMeas.sqlColumn, baseMeas.agg);
              expanded = expanded.replaceAll(fullRef, baseExpr);
            } else 
              expanded = expanded.replaceAll(fullRef, `${tAlias}.${fName}`);
          }
        }
        return expanded;
      };

      const compileNode = (node, contextLoc = 'filters clause') => {
        if (!node) return '';

        if (typeof node === 'string') {
          this.validateFilterSyntax(node);
          validateSqlExpression(node, `${contextLoc} ("${node}")`);
          return node;
        }

        if (Array.isArray(node)) {
          const compiledParts = node.map((item, idx) => compileNode(item, `${contextLoc} item #${idx + 1}`)).filter(Boolean);
          if (compiledParts.length === 0) return '';
          if (compiledParts.length === 1) return compiledParts[0];
          return compiledParts.join('\n  AND ');
        }

        if (typeof node === 'object' && node !== null) {
          const keys = Object.keys(node);
          if (keys.length === 0) return '';

          const key = keys[0];
          const lowerKey = key.toLowerCase();

          if (lowerKey === 'or' || lowerKey === 'and') {
            const val = node[key], op = lowerKey.toUpperCase();
            const items = Array.isArray(val) ? val : [val];

            const compiledSub = items.map((sub, i) => compileNode(sub, `${contextLoc} -> ${op}[${i}]`)).filter(Boolean);

            if (compiledSub.length === 0) return '';
            if (compiledSub.length === 1) return compiledSub[0];

            return `(${compiledSub.join(` ${op} `)})`;
          } else 
            throw new Error(`Invalid logic block '${key}' in filter. Expected 'or' or 'and'.`);
        }

        return '';
      };

      (model.relationships || []).forEach(rel => {
        if (rel.sql_on) {
          this.validateNoTrailingColon(rel.sql_on, `relationship '${rel.name || 'unnamed'}' (sql_on)`);
          validateSqlExpression(rel.sql_on, `relationship '${rel.name || 'unnamed'}' (sql_on)`);
        }
      });

      let hasMeasures = false;
      const compiledWhereClause = compileNode(model.filters);
      const selectClauses = [], groupByClauses = [];

      normalizedTables.forEach(tableDef => {
        const tAlias = tableDef.alias;

        tableDef.dimensions.forEach(dim => {
          if (!dim.sqlColumn || !dim.alias) return;
          const expr = this.qualifyDimensionExpr(dim.sqlColumn, tAlias);
          selectClauses.push(`  ${expr} AS ${tAlias}_${dim.alias}`);
          groupByClauses.push(`  ${expr}`);
        });

        tableDef.baseMeasures.forEach(meas => {
          if (!meas.sqlColumn || !meas.alias) return;
          hasMeasures = true;
          const expr = this.buildBaseMeasureSQL(tAlias, meas.sqlColumn, meas.agg);
          selectClauses.push(`  ${expr} AS ${tAlias}_${meas.alias}`);
        });

        tableDef.calculatedMeasures.forEach(calc => {
          if (!calc.formula || !calc.alias) return;
          hasMeasures = true;
          validateSqlExpression(calc.formula, `calculated measure '${calc.alias}' in '${tAlias}'`);
          const expandedExpr = expandCalculatedFormula(calc.formula);
          selectClauses.push(`  (${expandedExpr}) AS ${tAlias}_${calc.alias}`);
        });
      });

      const joinClauses = [];
      const joinedAliases = new Set([baseTableAlias]), unjoinedTables = normalizedTables.slice(1);

      while (unjoinedTables.length > 0) {
        let progressMade = false;

        for (let i = 0; i < unjoinedTables.length; i++) {
          const targetTable = unjoinedTables[i];
          const tAlias = targetTable.alias;

          const rel = (model.relationships || []).find(r => {
            const fromJoined = joinedAliases.has(r.from_table);
            const toJoined = joinedAliases.has(r.to_table);
            return (fromJoined && r.to_table === tAlias) || (toJoined && r.from_table === tAlias);
          });

          if (rel) {
            const aliasPart = targetTable.sqlTable !== targetTable.alias ? ` AS ${tAlias}` : '';
            joinClauses.push(`${rel.join_type || 'LEFT'} JOIN ${targetTable.sqlTable}${aliasPart}\n  ON ${rel.sql_on}`);
            
            joinedAliases.add(tAlias);
            unjoinedTables.splice(i, 1);
            progressMade = true;
            break;
          }
        }

        if (!progressMade) {
          const missing = unjoinedTables.map(t => t.alias).join(', ');
          throw new Error(`Cannot resolve join path for table(s): [<b>${missing}</b>]. Check that relationships connect them to an already joined table.`);
        }
      }

      const baseAliasPart = baseTableObj.sqlTable !== baseTableObj.alias ? ` AS ${baseTableAlias}` : '';
      let sql = `SELECT\n${selectClauses.join(',\n')}\nFROM ${baseTableObj.sqlTable}${baseAliasPart}`;

      if (joinClauses.length > 0) 
        sql += `\n${joinClauses.join('\n')}`;

      if (compiledWhereClause) 
        sql += `\nWHERE\n  ${compiledWhereClause}`;

      if (hasMeasures && groupByClauses.length > 0) 
        sql += `\nGROUP BY\n${groupByClauses.join(',\n')}`;

      if (this.obj.sqlOutput) 
        this.obj.sqlOutput.textContent = sql + ';';

    } catch (err) {
      if (this.obj.errorBox) {
        this.obj.errorBox.innerHTML = `⚠️ <b>YAML Compiler Error:</b> ${err.message}`;
        this.obj.errorBox.style.display = 'block';
      }
    }
  }

  copySQL() {
    if (this.obj.sqlOutput) {
      const text = this.obj.sqlOutput.textContent;
      navigator.clipboard.writeText(text);
      alert('SQL copied to clipboard!');
    }
  }

}