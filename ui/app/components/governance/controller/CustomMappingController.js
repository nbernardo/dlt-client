import { BaseController } from "../../../../@still/component/super/service/BaseController.js";
import { AppTemplate } from "../../../../config/app-template.js";
import { InputDropdown } from "../../../util/InputDropdownUtil.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { GovernanceMainComponent } from "../GovernanceMainComponent.js";

export class CustomMappingController extends BaseController {

  /** @type { GovernanceMainComponent } */ obj;
  editor;
  selectedConnection = '';
  databaseSchema = {};
  tableNames = [];
  selectedTable = '';
  selectedField = '';
  activeTagName = '';

  /** @type { InputDropdown } */ tableDropdown;
  /** @type { InputDropdown } */ fieldDropdown;
  /** @Prop */ dataQuery;

  xmlContent = `<?xml version="1.0" encoding="UTF-8"?>
<DataMapping>
  <CustomerInfo>
    <CustomCustomerID></CustomCustomerID>
    <CustomCustomerName></CustomCustomerName>
  </CustomerInfo>
  <OrderInfo>
    <CustomOrderID></CustomOrderID>
  </OrderInfo>
</DataMapping>`;

  /** @returns { HTMLElement } */ $ = (ref) => this.obj.container.querySelector(ref);
  /** @returns { HTMLElement } */ $$ = (ref) => this.obj.container.querySelectorAll(ref);

  async init() {
    this.initEditor();
    this.initCompletionProvider();

    const secretList = await WorkspaceService.listSecrets(1 /** 1 = DBSecrets */);
    this.obj.sourceConnections = secretList;

    // Force Monaco layout refresh after the tab container is rendered/visible
    setTimeout(() => this.editor ? this.editor.layout() : '', 150);
  }

  initEditor() {
    const container = this.$('#custom-xml-editor');
    if (!container || this.editor) return;

    this.editor = monaco.editor.create(container, {
      value: this.xmlContent,
      language: 'xml',
      theme: 'vs-light',
      automaticLayout: true,
      minimap: { enabled: false },
      scrollBeyondLastLine: false,
      fontSize: 13,
      quickSuggestions: { other: true, comments: false, strings: true }
    });

    this.editor.onDidChangeModelContent(() => this.xmlContent = this.editor.getValue());
    this.editor.onDidChangeCursorPosition(() => this.updateCursorTagFromEditor());
    this.updateCursorTagFromEditor();
  }

  normalizeColumns(raw = []) {
    return raw.map(c => typeof c === 'string' ? c : (c.column_name || c.name || c.field || String(c)) );
  }

  initCompletionProvider() {
    CustomMappingController._completionDisposable?.dispose();

    // Command executed by Monaco right after a field suggestion is accepted
    CustomMappingController._sqlCommandDisposable?.dispose();
    CustomMappingController._sqlCommandDisposable = monaco.editor.registerCommand('customMapping.logSql', () => this.logSqlQueries());

    CustomMappingController._completionDisposable = monaco.languages.registerCompletionItemProvider('xml', {
        // '{' opens the table list, '.' opens the field list
        triggerCharacters: ['{', '.'],

        provideCompletionItems: (model, position) => {
          const empty = { suggestions: [] };

          if (!this.editor || model !== this.editor.getModel()) return empty;

          // Only inside tag content (not inside a tag/attribute)
          const textUntilCursor = model.getValueInRange({
            startLineNumber: 1, startColumn: 1,
            endLineNumber: position.lineNumber, endColumn: position.column
          });
          const lastOpen = textUntilCursor.lastIndexOf('<');
          const lastClose = textUntilCursor.lastIndexOf('>');
          if (lastClose === -1 || lastOpen > lastClose) return empty;

          const offset = model.getOffsetAt(position);
          if (!this.findEnclosingXmlTag(model.getValue(), offset)) return empty;

          const lineContent = model.getLineContent(position.lineNumber);
          const before = lineContent.substring(0, position.column - 1);
          const after = lineContent.substring(position.column - 1);

          // Does a '}' already follow the cursor? (Monaco may auto-close brackets)
          const hasClosingBrace = after.startsWith('}');

          // 1) "{table." or "{table.partialField" -> fields
          const fieldMatch = before.match(/\{([A-Za-z0-9_-]+)\.([A-Za-z0-9_]*)$/);
          if (fieldMatch) {
            const typedTable = fieldMatch[1];
            const typedField = fieldMatch[2];

            const tableKey = Object.keys(this.databaseSchema)
              .find(t => t.toLowerCase() === typedTable.toLowerCase());
            if (!tableKey) return empty;

            const range = {
              startLineNumber: position.lineNumber,
              endLineNumber: position.lineNumber,
              startColumn: position.column - typedField.length,
              endColumn: position.column
            };

            return {
              suggestions: this.normalizeColumns(this.databaseSchema[tableKey]).map(col => ({
                label: col,
                kind: monaco.languages.CompletionItemKind.Field,
                insertText: hasClosingBrace ? col : col + '}',
                command: { id: 'customMapping.logSql', title: 'Log SQL' },
                detail: `Column of '${tableKey}'`,
                range
              }))
            };
          }

          // 2) "{" or "{partialTable" -> tables (all of them when nothing is typed yet)
          const tableMatch = before.match(/\{([A-Za-z0-9_-]*)$/);
          if (tableMatch) {
            const typedTable = tableMatch[1];

            const range = {
              startLineNumber: position.lineNumber,
              endLineNumber: position.lineNumber,
              startColumn: position.column - typedTable.length,
              endColumn: position.column
            };

            return {
              suggestions: this.tableNames.map(tbl => ({
                label: tbl,
                kind: monaco.languages.CompletionItemKind.Class,
                insertText: tbl + '.',
                command: { id: 'editor.action.triggerSuggest', title: 'Suggest fields' },
                detail: 'Table from connection',
                range
              }))
            };
          }
          // Not inside a "{...}" expression -> no DB intellisense
          return empty;
        }
      });
  }

  updateCursorTagFromEditor() {
    if (!this.editor) return;
    const position = this.editor.getPosition();
    const model = this.editor.getModel();
    if (!position || !model) return;

    const offset = model.getOffsetAt(position);
    const text = model.getValue();
    const activeTag = this.findEnclosingXmlTag(text, offset);

    const tagDisplay = this.$('#active-xml-tag');
    if (tagDisplay) {
      if (activeTag) {
        this.activeTagName = activeTag;
        tagDisplay.textContent = `<${activeTag}>`;
        tagDisplay.className = 'badge badge-teal';
      } else {
        this.activeTagName = '';
        tagDisplay.textContent = 'None (Place cursor inside an XML tag)';
        tagDisplay.className = 'text-muted';
      }
    }
  }

  async onConnectionChange(connectionName) {
    this.selectedConnection = connectionName;
    if (!connectionName) {
      this.databaseSchema = {};
      this.tableNames = [];
      this.tableDropdown?.setDataSource([]);
      this.fieldDropdown?.setDataSource([]);
      return;
    }

    try {
      const data = await WorkspaceService.getConnectionDetails(connectionName, true);
      this.databaseSchema = data.tables || {};
      this.tableNames = Object.keys(this.databaseSchema);
      if (this.tableDropdown) this.tableDropdown.setDataSource(this.tableNames);
    } catch (err) {
      AppTemplate.toast.error("Error loading connection metadata: " + err.message);
    }
  }

  updateFieldOptions(tableName) {
    const columns = this.normalizeColumns(this.databaseSchema[tableName] || []);
    this.fieldDropdown?.setDataSource(columns);
  }

  findEnclosingXmlTag(xml, offset) {
    const stack = [];
    const tagRegex = /<\/?([a-zA-Z0-9_-]+)[^>]*>/g;
    let match;

    while ((match = tagRegex.exec(xml)) !== null) {
      const isClosing = match[0].startsWith('</');
      const isSelfClosing = match[0].endsWith('/>');
      const tagName = match[1];
      const matchStart = match.index;
      const matchEnd = match.index + match[0].length;

      if (isClosing) {
        if (stack.length && stack[stack.length - 1].name === tagName) {
          const popped = stack.pop();
          if (offset >= popped.start && offset <= matchEnd) return tagName;
        }
      } else if (!isSelfClosing) 
        stack.push({ name: tagName, start: matchStart });
    }

    return stack.length ? stack[stack.length - 1].name : null;
  }

  async saveCustomMapping() {
    const payload = {
      type: 'xml', connectionName: this.selectedConnection, 
      templateName: this.obj.templateMappingName.value,
      xmlConfig: this.editor ? this.editor.getValue() : this.xmlContent,
      dataQuery: Object.values(this.buildSqlQueries())[0] || '',
    };
    
    let result = await this.obj.serviceController.saveMappedTemplate(payload);
    result = await result.json()
    if(result.success) return AppTemplate.toast.success('Custom data mapping saved successfully');
    AppTemplate.toast.error(result.result.error)
  }

  /** Collects every {table.field} in the XML, grouped by table (deduplicated). */
  extractMappedFields(xml) {
    const regex = /\{([A-Za-z0-9_-]+)\.([A-Za-z0-9_]+)\}/g, byTable = {};
    let m;

    while ((m = regex.exec(xml)) !== null) {
      // Resolve to the real table name (case-insensitive) and skip unknown ones
      const tableKey = Object.keys(this.databaseSchema)
        .find(t => t.toLowerCase() === m[1].toLowerCase());
      if (!tableKey) continue;

      // Resolve to the real column name and skip unknown ones
      const column = this.normalizeColumns(this.databaseSchema[tableKey])
        .find(c => c.toLowerCase() === m[2].toLowerCase());
      if (!column) continue;

      (byTable[tableKey] ??= new Set()).add(column);
    }
    return byTable;
  }

  /** Returns { tableName: 'SELECT ...' } for the tables/fields used in the editor. */
  buildSqlQueries() {
    const xml = this.editor ? this.editor.getValue() : this.xmlContent;
    const byTable = this.extractMappedFields(xml), queries = {};

    for (const [table, fields] of Object.entries(byTable)) {
      queries[table] = `SELECT ${[...fields].join(', ')} FROM ${table};`;
    }
    return queries;
  }

  logSqlQueries() {
    this.dataQuery = this.buildSqlQueries();
    const list = Object.values(this.dataQuery);

    if (!list.length) {
      console.log('[CustomMapping] No {table.field} mappings yet.');
      return;
    }
    console.log('[CustomMapping] Generated SQL:\n' + list.join('\n'));
  }
}