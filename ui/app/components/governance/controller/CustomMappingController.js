import { BaseController } from "../../../../@still/component/super/service/BaseController.js";
import { AppTemplate } from "../../../../config/app-template.js";
import { InputDropdown } from "../../../util/InputDropdownUtil.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { GovernanceMainComponent } from "../GovernanceMainComponent.js";
import { CustomMappingFormController } from "./CustomMappingFormController.js";
import jsyaml from "https://cdn.jsdelivr.net/npm/js-yaml@4.1.0/dist/js-yaml.mjs";
import { XMLParser, XMLValidator } from "https://cdn.jsdelivr.net/npm/fast-xml-parser@5.11.2/+esm";

export class CustomMappingController extends BaseController {

  /** @type { GovernanceMainComponent } */ obj;
  editor;
  /** Read-only Monaco showing the XML generated from the YAML */
  xmlEditor;
  selectedConnection = '';
  databaseSchema = {};
  tableNames = [];
  selectedTable = '';
  selectedField = '';

  /** Tables offered by the intellisense (empty = all tables of the connection) */
  allowedTables = new Set();
  /** One-to-many relationships: { parentTable, parentColumn, childTable, childColumn } ("one" = parent) */
  relationships = [];
  /** Relationship being built in the form */
  relDraft = { parentTable: '', parentColumn: '', childTable: '', childColumn: '' };
  tableFilter = '';
  /** Also select the parent key of every join, so each child row can be grouped under its parent */
  includeJoinKeys = true;

  /** Visual (form) mode of the editor: another view over the same YAML */
  /** @type { CustomMappingFormController } */ formView;

  /** @type { InputDropdown } */ tableDropdown;
  /** @type { InputDropdown } */ fieldDropdown;
  /** @Prop */ dataQuery;

  /** Source XML (replace with the XML supplied for the template, or call loadXml(xml)) */
  xmlContent = `<?xml version="1.0" encoding="UTF-8"?>
<MNDTImf:InputMandateFile xmlns:MNDTImf="urn:MNDTImf:xsd:$MPEMndt">
  <MNDTImf:SndgInst>EXIAAOL0</MNDTImf:SndgInst>
  <MNDTImf:RcvgInst>CCAAAOL0</MNDTImf:RcvgInst>
  <MNDTImf:UltmtCdtrId>AO5900112233</MNDTImf:UltmtCdtrId>
  <MNDTImf:FileRef>D202611100000010</MNDTImf:FileRef>
  <MNDTImf:SrvcId>ADC</MNDTImf:SrvcId>
  <MNDTImf:TstCode>T</MNDTImf:TstCode>
  <MNDTImf:FType>IMF</MNDTImf:FType>
  <MNDTImf:FDtTm>2026-11-10T10:50:01Z</MNDTImf:FDtTm>
  <MNDTImf:Num009CAPBlk>1</MNDTImf:Num009CAPBlk>
  <MNDTImf:Num009SAPBlk>0</MNDTImf:Num009SAPBlk>
  <MNDTImf:Num012CAPBlk>0</MNDTImf:Num012CAPBlk>
  <MNDTImf:Num012SAPBlk>0</MNDTImf:Num012SAPBlk>
  <MNDTImf:Num010Blk>0</MNDTImf:Num010Blk>
  <MNDTImf:Num011Blk>0</MNDTImf:Num011Blk>
  <MNDTImf:Pain009CAP xmlns="urn:iso:std:iso:20022:tech:xsd:pain.009.001.05">
    <GrpHdr>
      <MsgId></MsgId>
      <CreDtTm></CreDtTm>
      <InstgAgt>
        <FinInstnId>
          <BICFI></BICFI>
        </FinInstnId>
      </InstgAgt>
      <InstdAgt>
        <FinInstnId>
          <BICFI></BICFI>
        </FinInstnId>
      </InstdAgt>
    </GrpHdr>

    <Mndt>
      <MndtId></MndtId>
      <MndtReqId></MndtReqId>
      <Tp>
        <SvcLvl><Cd></Cd></SvcLvl>
        <LclInstrm><Cd></Cd></LclInstrm>
        <CtgyPurp><Cd></Cd></CtgyPurp>
        <Clssfctn><Prtry></Prtry></Clssfctn>
      </Tp>
      <Ocrncs>
        <SeqTp></SeqTp>
        <Frqcy>
          <PtInTm>
            <Tp></Tp>
            <PtInTm></PtInTm>
          </PtInTm>
        </Frqcy>
        <FrstColltnDt></FrstColltnDt>

      </Ocrncs>
      <TrckgInd>true</TrckgInd>

      <Cdtr>
        <Nm>Timeboxed</Nm>
        <Id><OrgId><Othr><Id></Id></Othr></OrgId></Id>
      </Cdtr>
      <CdtrAcct><Id><IBAN></IBAN></Id></CdtrAcct>
      <CdtrAgt><FinInstnId><BICFI></BICFI></FinInstnId></CdtrAgt>
      <Dbtr>
        <Nm>Diolene Sampaio</Nm>
        <Id><OrgId><Othr><Id></Id></Othr></OrgId></Id>
        <CtctDtls>
          <MobNb></MobNb>
          <EmailAdr></EmailAdr>
          <Othr></Othr>
        </CtctDtls>
      </Dbtr>
      <DbtrAcct><Id><IBAN></IBAN></Id></DbtrAcct>
      <DbtrAgt><FinInstnId><BICFI></BICFI></FinInstnId></DbtrAgt>
      <RfrdDoc>
        <CdtrRef></CdtrRef>
        <RltdDt></RltdDt>
      </RfrdDoc>
      <SplmtryData>
        <Envlp>
          <Img>
            <FrntImg></FrntImg>
          </Img>
        </Envlp>
      </SplmtryData>
    </Mndt>

  </MNDTImf:Pain009CAP>
</MNDTImf:InputMandateFile>
`;

  /** Derived from xmlContent in init() / loadXml() */
  yamlContent = '';

  /** @returns { HTMLElement } */ $ = (ref) => this.obj.container.querySelector(ref);
  /** @returns { HTMLElement } */ $$ = (ref) => this.obj.container.querySelectorAll(ref);

  async init() {
    try { this.yamlContent = this.xmlToYaml(this.xmlContent); }
    catch (err) { AppTemplate.toast.error('Invalid source XML: ' + err.message); }

    this.initEditor();
    this.initXmlEditor();
    this.initCompletionProvider();
    this.initConfigPanels();
    this.formView ??= new CustomMappingFormController(this);
    this.formView.init();   // wires the YAML / Visual switch
    this.showSourceXml();   // right side starts as the original XML; it is regenerated as the YAML is edited

    const secretList = await WorkspaceService.listSecrets(1 /** 1 = DBSecrets */);
    this.obj.sourceConnections = secretList;

    // Force Monaco layout refresh after the tab container is rendered/visible
    setTimeout(() => { this.editor?.layout(); this.xmlEditor?.layout(); }, 150);
  }

  initEditor() {
    // Accepts the new id, falls back to the old one if the HTML wasn't renamed
    const container = this.$('#custom-yaml-editor') || this.$('#custom-xml-editor');
    if (!container || this.editor) return;

    this.editor = monaco.editor.create(container, {
      value: this.yamlContent, language: 'yaml', theme: 'vs-light', automaticLayout: true, minimap: { enabled: false },
      scrollBeyondLastLine: false, fontSize: 13, tabSize: 2, insertSpaces: true, fixedOverflowWidgets: true,
      wordBasedSuggestions: 'off', quickSuggestions: { other: true, comments: false, strings: true }
    });

    this.editor.onDidChangeModelContent(() => {
      this.yamlContent = this.editor.getValue();
      clearTimeout(this._xmlTimer);
      this._xmlTimer = setTimeout(() => this.renderXmlPreview(), 250);   // live XML output
    });
  }

  /** Read-only editor on the right side that shows the generated XML */
  initXmlEditor() {
    const container = this.$('#custom-xml-output');
    if (!container || this.xmlEditor) return;

    this.xmlEditor = monaco.editor.create(container, {
      value: '',
      language: 'xml',
      theme: 'vs-light',
      readOnly: true,
      domReadOnly: true,
      automaticLayout: true,
      minimap: { enabled: false },
      scrollBeyondLastLine: false,
      fontSize: 13,
      wordWrap: 'on',
      renderLineHighlight: 'none',
      fixedOverflowWidgets: true
    });
  }

  /** Converts the current YAML and shows it in the XML editor.
   *  While the YAML is invalid (e.g. half-typed "{actor.") the last valid XML stays visible. */
  renderXmlPreview() {
    if (!this.xmlEditor) return;
    const status = this.$('#custom-xml-status');

    try {
      const xml = this.yamlToXml(this.editor ? this.editor.getValue() : this.yamlContent);

      if (this.xmlEditor.getValue() !== xml) {
        const scrollTop = this.xmlEditor.getScrollTop();
        this.xmlEditor.setValue(xml);              // programmatic updates work on a read-only editor
        this.xmlEditor.setScrollTop(scrollTop);    // don't jump to the top on every keystroke
      }
      if (status) { status.textContent = 'XML is in sync with the YAML'; status.style.color = '#2e7d32'; }
    } catch (err) {
      if (status) {
        status.textContent = 'Invalid YAML, showing last valid XML: ' + String(err?.message || err).split('\n')[0];
        status.style.color = '#b26a00';
      }
    }
  }

  normalizeColumns = (raw = []) =>
     raw.map(c => typeof c === 'string' ? c : (c.column_name || c.name || c.field || String(c)) );

  initCompletionProvider() {
    CustomMappingController._completionDisposable?.dispose();

    // Command executed by Monaco right after a field suggestion is accepted
    CustomMappingController._sqlCommandDisposable?.dispose();
    CustomMappingController._sqlCommandDisposable = monaco.editor.registerCommand('customMapping.logSql', () => this.logSqlQueries());

    CustomMappingController._completionDisposable = monaco.languages.registerCompletionItemProvider('yaml', {
        // '{' opens the table list, '.' opens the field list
        triggerCharacters: ['{', '.'],

        provideCompletionItems: (model, position) => {
          const empty = { suggestions: [] };

          if (!this.editor || model !== this.editor.getModel()) return empty;

          const content = model.getLineContent(position.lineNumber);
          const before = content.substring(0, position.column - 1), after = content.substring(position.column - 1);

          // Only in the VALUE of a property:  "key: ..." or "- key: ..."
          if (!/^\s*(?:-\s+)?[^\s:#'"][^#]*?:\s+/.test(before)) return empty;

          // Does a '}' already follow the cursor? (Monaco may auto-close brackets)
          const hasClosingBrace = after.startsWith('}');

          // Text typed after the last "{". Table names can contain dots (e.g. public.sale_order)
          const typedMatch = before.match(/\{([A-Za-z0-9_.-]*)$/);
          if (!typedMatch) return empty;

          const typed = typedMatch[1];
          const available = this.getAvailableTables();

          // 1) "{schema.table." or "{schema.table.partialField" -> fields (the longest matching table name wins)
          const tableKey = this.findTablePrefix(typed, available);
          if (tableKey) {
            const typedField = typed.slice(tableKey.length + 1);
            if (!/^[A-Za-z0-9_]*$/.test(typedField)) return empty;

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

          // 2) "{" or "{partial.table.name" -> tables (all of them when nothing is typed yet)
          const range = {
            startLineNumber: position.lineNumber,
            endLineNumber: position.lineNumber,
            startColumn: position.column - typed.length,
            endColumn: position.column
          };

          return {
            suggestions: available.map(tbl => ({
              label: tbl,
              kind: monaco.languages.CompletionItemKind.Class,
              insertText: tbl + '.',
              command: { id: 'editor.action.triggerSuggest', title: 'Suggest fields' },
              detail: 'Table from connection',
              range
            }))
          };
        }
      });
  }

  async onConnectionChange(connectionName) {
    this.selectedConnection = connectionName;
    this.resetConfig();
    if (!connectionName) {
      this.databaseSchema = {};
      this.tableNames = [];
      this.tableDropdown?.setDataSource([]);
      this.fieldDropdown?.setDataSource([]);
      this.renderConfigPanels();
      return;
    }

    try {
      const data = await WorkspaceService.getConnectionDetails(connectionName, true);
      this.databaseSchema = data.tables || {};
      this.tableNames = Object.keys(this.databaseSchema);
      if (this.tableDropdown) this.tableDropdown.setDataSource(this.tableNames);
      this.renderConfigPanels();
    } catch (err) {
      AppTemplate.toast.error("Error loading connection metadata: " + err.message);
    }
  }

  updateFieldOptions(tableName) {
    const columns = this.normalizeColumns(this.databaseSchema[tableName] || []);
    this.fieldDropdown?.setDataSource(columns);
  }

  async saveCustomMapping() {
    const yamlConfig = this.editor ? this.editor.getValue() : this.yamlContent;
    const xmlConfig = this.logXml(); // YAML converted to XML, null if the YAML is invalid

    if (!xmlConfig) return AppTemplate.toast.error('Fix the YAML errors before saving.');

    const payload = {
      type: 'xml', connectionName: this.selectedConnection, templateName: this.obj.templateMappingName.value,
      xmlConfig, yamlConfig, dataQuery: Object.values(this.buildSqlQueries())[0] || '',
      dataQueries: this.buildSqlQueries(), // one query per hierarchy (root table)
      allowedTables: [...this.allowedTables], relationships: this.relationships,
    };
    
    let result = await this.obj.serviceController.saveMappedTemplate(payload);
    result = await result.json()
    if(result.success) return AppTemplate.toast.success('Custom data mapping saved successfully');
    AppTemplate.toast.error(result.result.error)
  }

  /* ------------------ Available tables & relationships ------------------ */
  /** Tables offered by the intellisense: the selected ones (all of them while none is selected) */
  getAvailableTables() {
    return this.allowedTables.size
      ? this.tableNames.filter(t => this.allowedTables.has(t)) : this.tableNames;
  }

  /** Relationships whose two tables are both available */
  getActiveRelationships() {
    const avail = new Set(this.getAvailableTables());
    return this.relationships.filter(r => avail.has(r.parentTable) && avail.has(r.childTable));
  }

  /** Longest table name T such that text starts with "T." (case-insensitive), or null */
  findTablePrefix(text, tables) {
    const low = text.toLowerCase();
    return tables.filter(t => low.startsWith(t.toLowerCase() + '.')).sort((a, b) => b.length - a.length)[0] || null;
  }

  columnsOf = (table) =>  table ? this.normalizeColumns(this.databaseSchema[table] || []) : [];

  escapeHtml = (v) =>  String(v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

  emptyDraft = () => ({ parentTable: '', parentColumn: '', childTable: '', childColumn: '' });
  
  /** State only (no rendering): used when the connection changes */
  resetConfig() {
    this.allowedTables = new Set();
    this.relationships = [];
    this.relDraft = this.emptyDraft();
    this.tableFilter = '';
  }

  /** Public: restore a saved selection. Call it after onConnectionChange() has finished for that connection. */
  loadConfig({ allowedTables = [], relationships = [] } = {}) {
    this.allowedTables = new Set(allowedTables.filter(t => this.tableNames.includes(t)));
    this.relationships = relationships.filter(r => this.databaseSchema[r.parentTable] && this.databaseSchema[r.childTable]);
    this.relDraft = this.emptyDraft();
    this.renderConfigPanels();
  }

  /** Builds the (static) skeleton of both panels once and wires their events */
  initConfigPanels() {
    const picker = this.$('#custom-tables-picker');
    if (picker && !picker.dataset.ready) {
      picker.dataset.ready = '1';
      picker.innerHTML = `
        <input type="text" id="cm-table-filter" placeholder="Filter tables...">
        <div class="cm-actions">
          <button type="button" class="btn btn-sm" id="cm-tables-all">Select shown</button>
          <button type="button" class="btn btn-sm" id="cm-tables-none">Clear</button>
        </div>
        <div id="cm-table-list" class="cm-list"></div><div id="cm-table-hint" class="cm-hint"></div>`;

      picker.addEventListener('input', e => {
        if (e.target.id !== 'cm-table-filter') return;
        this.tableFilter = e.target.value;
        this.renderTableList();
      });
      picker.addEventListener('change', e => {
        const table = e.target.dataset?.table;
        if (table) this.toggleTable(table, e.target.checked);
      });
      picker.addEventListener('click', e => {
        if (e.target.closest('#cm-tables-all')) this.selectShownTables(true);
        else if (e.target.closest('#cm-tables-none')) this.selectShownTables(false);
      });
    }

    const rel = this.$('#custom-relationships');
    if (rel && !rel.dataset.ready) {
      rel.dataset.ready = '1';
      rel.innerHTML = `
        <div class="cm-rel-form">
          <span class="cm-side-label">One (parent)</span>
          <div class="cm-side"><select id="cm-rel-ptable"></select><select id="cm-rel-pcol"></select></div>
          <span class="cm-side-label">Many (child)</span>
          <div class="cm-side"><select id="cm-rel-ctable"></select><select id="cm-rel-ccol"></select></div>
          <button type="button" class="btn btn-primary btn-sm" id="cm-rel-add" style="width:auto;">Add relationship</button>
        </div>
        <div id="cm-rel-list" class="cm-rel-list"></div>`;

      rel.addEventListener('change', e => {
        const d = this.relDraft;
        switch (e.target.id) {
          case 'cm-rel-ptable': d.parentTable = e.target.value; d.parentColumn = ''; break;
          case 'cm-rel-pcol':   d.parentColumn = e.target.value; break;
          case 'cm-rel-ctable': d.childTable = e.target.value; d.childColumn = ''; break;
          case 'cm-rel-ccol':   d.childColumn = e.target.value; break;
          default: return;
        }
        this.renderRelationshipForm();
      });
      rel.addEventListener('click', e => {
        if (e.target.closest('#cm-rel-add')) return this.addRelationship();
        const rm = e.target.closest('[data-remove]');
        if (rm) this.removeRelationship(Number(rm.dataset.remove));
      });
    }

    this.renderConfigPanels();
  }

  renderConfigPanels() {
    this.renderTableList();
    this.renderRelationshipForm();
    this.renderRelationshipList();
  }

  renderTableList() {
    const list = this.$('#cm-table-list');
    if (!list) return;

    const f = this.tableFilter.trim().toLowerCase();
    const visible = this.tableNames.filter(t => t.toLowerCase().includes(f));

    list.innerHTML = visible.length
      ? visible.map(t => `<label class="cm-item"><input type="checkbox" data-table="${this.escapeHtml(t)}"${this.allowedTables.has(t) ? ' checked' : ''}> <span>${this.escapeHtml(t)}</span></label>`).join('')
      : `<span class="text-muted" style="font-size:11px;">${this.tableNames.length ? 'No table matches the filter' : 'Select a connection to load its tables'}</span>`;

    const hint = this.$('#cm-table-hint');
    if (hint) {
      const n = this.allowedTables.size;
      hint.textContent = n
        ? `${n} table(s) available in the intellisense` : (this.tableNames.length ? 'No table selected: all tables are available' : '');
    }
  }

  toggleTable(table, on) {
    on ? this.allowedTables.add(table) : this.allowedTables.delete(table);
    this.onTablesChanged();
  }

  selectShownTables(on) {
    const f = this.tableFilter.trim().toLowerCase();
    this.tableNames.filter(t => t.toLowerCase().includes(f))
      .forEach(t => on ? this.allowedTables.add(t) : this.allowedTables.delete(t));
    this.renderTableList();
    this.onTablesChanged();
  }

  onTablesChanged() {
    this.renderTableList();
    this.renderRelationshipForm();
    this.renderRelationshipList(); // relationships whose tables are not selected show as inactive
  }

  renderRelationshipForm() {
    const tables = this.getAvailableTables(), d = this.relDraft;
    if (!tables.includes(d.parentTable)) { d.parentTable = ''; d.parentColumn = ''; }
    if (!tables.includes(d.childTable))  { d.childTable = '';  d.childColumn = ''; }

    const fill = (id, options, current, placeholder) => {
      const el = this.$(id);
      if (!el) return;
      el.innerHTML = `<option value="">${placeholder}</option>` + options
        .map(o => `<option value="${this.escapeHtml(o)}"${o === current ? ' selected' : ''}>${this.escapeHtml(o)}</option>`)
        .join('');
    };

    fill('#cm-rel-ptable', tables, d.parentTable, 'Table');
    fill('#cm-rel-pcol', this.columnsOf(d.parentTable), d.parentColumn, 'Column');
    fill('#cm-rel-ctable', tables, d.childTable, 'Table');
    fill('#cm-rel-ccol', this.columnsOf(d.childTable), d.childColumn, 'Column');
  }

  renderRelationshipList() {
    const list = this.$('#cm-rel-list');
    if (!list) return;

    const active = new Set(this.getActiveRelationships());
    list.innerHTML = this.relationships.length
      ? this.relationships.map((r, i) => `
          <div class="cm-rel-item${active.has(r) ? '' : ' cm-inactive'}"${active.has(r) ? '' : ' title="Inactive: one of the tables is not selected"'}>
            <span><b>${this.escapeHtml(r.parentTable)}</b>.${this.escapeHtml(r.parentColumn)} (1)
              &rarr; <b>${this.escapeHtml(r.childTable)}</b>.${this.escapeHtml(r.childColumn)} (N)</span>
            <button type="button" data-remove="${i}" title="Remove">&times;</button>
          </div>`).join('')
      : '<span class="text-muted" style="font-size:11px;">No relationships: each table is queried on its own</span>';
  }

  /** True when "to" can be reached from "from" going parent -> child */
  reaches(from, to, seen = new Set()) {
    if (from === to) return true;
    if (seen.has(from)) return false;
    seen.add(from);
    return this.relationships.some(r => r.parentTable === from && this.reaches(r.childTable, to, seen));
  }

  addRelationship() {
    const { parentTable, parentColumn, childTable, childColumn } = this.relDraft;

    if (!parentTable || !parentColumn || !childTable || !childColumn)
      return AppTemplate.toast.warn('Select the parent table/column and the child table/column.');
    if (parentTable === childTable)
      return AppTemplate.toast.warn('A table cannot be related to itself.');
    if (this.relationships.some(r => r.parentTable === parentTable && r.childTable === childTable))
      return AppTemplate.toast.warn(`${parentTable} and ${childTable} are already related. Remove that relationship first.`);
    if (this.reaches(childTable, parentTable))
      return AppTemplate.toast.warn(`This would create a cycle: ${childTable} already leads to ${parentTable}.`);

    this.relationships.push({ parentTable, parentColumn, childTable, childColumn });
    this.relDraft = this.emptyDraft();
    this.renderRelationshipForm();
    this.renderRelationshipList();
    this.logSqlQueries();
  }

  removeRelationship(index) {
    this.relationships.splice(index, 1);
    this.renderRelationshipList();
    this.logSqlQueries();
  }

  /** Collects every {table.field} in the text, grouped by table (deduplicated). Table may be schema.table */
  extractMappedFields(text) {
    const regex = /\{([A-Za-z0-9_.-]+)\}/g, byTable = {};
    const available = this.getAvailableTables();
    let m;

    while ((m = regex.exec(text)) !== null) {
      // Resolve the real table name (case-insensitive); skip tables that are unknown or not selected
      const tableKey = this.findTablePrefix(m[1], available);
      if (!tableKey) continue;

      // Resolve the real column name; skip unknown ones
      const wanted = m[1].slice(tableKey.length + 1).toLowerCase();
      const column = this.columnsOf(tableKey).find(c => c.toLowerCase() === wanted);
      if (!column) continue;

      (byTable[tableKey] ??= new Set()).add(column);
    }
    return byTable;
  }

  /**
   * One plan per hierarchy for the tables used in the YAML.
   * The root is the table that reaches the most used tables going from "one" to "many";
   * tables in between are joined even if no field of them is mapped.
   * Returns [{ root, tables, joins, columns, sql }]
   */
  buildQueryPlans() {
    const yaml = this.editor ? this.editor.getValue() : this.yamlContent;
    const byTable = this.extractMappedFields(yaml);
    const used = Object.keys(byTable);
    if (!used.length) return [];

    const childrenOf = {};
    for (const rel of this.getActiveRelationships()) (childrenOf[rel.parentTable] ??= []).push(rel);

    // Walks from the "one" side down to the "many" side, remembering how each table was reached
    const walkDown = (root) => {
      const via = new Map([[root, null]]), order = [root];
      for (let i = 0; i < order.length; i++)
        for (const rel of childrenOf[order[i]] || [])
          if (!via.has(rel.childTable)) { via.set(rel.childTable, rel); order.push(rel.childTable); }
      return { via, order };
    };
    const better = (a, b) => { for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return a[i] > b[i]; return false; };

    const plans = [], uncovered = new Set(used);
    while (uncovered.size) {
      let best = null;
      for (const root of this.getAvailableTables()) {
        const { via, order } = walkDown(root);
        const covered = [...uncovered].filter(t => via.has(t));
        if (!covered.length) continue;
        // most used tables covered, then prefer a root that is itself used, then the smallest tree
        const rank = [covered.length, uncovered.has(root) ? 1 : 0, -order.length];
        if (!best || better(rank, best.rank)) best = { root, via, order, covered, rank };
      }
      if (!best) break;
      best.covered.forEach(t => uncovered.delete(t));
      plans.push(this.makePlan(best, byTable));
    }
    return plans;
  }

  makePlan({ root, via, order, covered }, byTable) {
    // Tables on the paths root -> each covered table
    const needed = new Set([root]);
    for (const t of covered) for (let x = t; x !== root; x = via.get(x).parentTable) needed.add(x);

    const tables = order.filter(t => needed.has(t)); // parents always before children
    const joins = tables.filter(t => t !== root).map(t => via.get(t));

    const columns = {};
    for (const t of tables) columns[t] = [...(byTable[t] || [])];
    if (this.includeJoinKeys)
      for (const j of joins) {
        const cols = columns[j.parentTable];
        if (!cols.includes(j.parentColumn)) cols.push(j.parentColumn);
      }

    let sql;
    if (!joins.length) {
      sql = `SELECT ${columns[root].join(', ')} FROM ${root};`;
    } else {
      const select = tables.flatMap(t => columns[t].map(c => `${t}.${c} AS ${t.replace(/\W/g, '_')}__${c}`));   // alias without the schema dot
      const joinSql = joins.map(j => `LEFT JOIN ${j.childTable} ON ${j.childTable}.${j.childColumn} = ${j.parentTable}.${j.parentColumn}`);
      const orderBy = [...new Set(joins.map(j => `${j.parentTable}.${j.parentColumn}`))];   // keeps children grouped under their parent
      sql = `SELECT ${select.join(', ')}\nFROM ${root}\n${joinSql.join('\n')}\nORDER BY ${orderBy.join(', ')};`;
    }

    return { root, tables, joins, columns, sql };
  }

  /** Returns { rootTable: 'SELECT ...' } (one query per hierarchy) */
  buildSqlQueries() {
    const queries = {};
    for (const plan of this.buildQueryPlans()) queries[plan.root] = plan.sql;
    return queries;
  }

  /** Runs after each field selection / relationship change: prints the SQL and then the XML */
  logSqlQueries() {
    const plans = this.buildQueryPlans();
    this.dataQuery = this.buildSqlQueries();

    console.log(plans.length
      ? '[CustomMapping] Generated SQL:\n' + plans.map(p => p.sql).join('\n\n')
      : '[CustomMapping] No {table.field} mappings yet.');

    for (const p of plans)
      if (p.joins.length)
        console.log('[CustomMapping] Hierarchy: ' + p.joins.map(j => `${j.parentTable}(1) -> ${j.childTable}(N)`).join(', '));

    this.logXml();   // prints the XML, or the YAML error if it can't be converted
  }

  /* ---------------------------- XML -> YAML ---------------------------- */
  /** XML -> YAML (fast-xml-parser builds the object, js-yaml dumps it) */
  xmlToYaml(xml) {
    const valid = XMLValidator.validate(xml);
    if (valid !== true) throw new Error(valid?.err?.msg || 'Invalid XML');

    const obj = new XMLParser({
      ignoreDeclaration: true, // drop <?xml ... ?>
      parseTagValue: false, // keep every value as text
      trimValues: true
    }).parse(xml);

    return jsyaml.dump(obj, { indent: 2, lineWidth: -1, noRefs: true })
      .replace(/: ''$/gm, ':')                                            // empty element -> "Key:"
      .replace(/: '(\{[A-Za-z0-9_.-]+\.[A-Za-z0-9_]+\})'$/gm, ': $1');     // keep {table.field} unquoted
  }

  /** Shows the original XML on the right side (no regeneration) */
  showSourceXml() {
    if (!this.xmlEditor) return;
    this.xmlEditor.setValue(this.xmlContent);
    const status = this.$('#custom-xml-status');
    if (status) { status.textContent = 'Showing the source XML'; status.style.color = '#555'; }
  }

  /** Public: load an XML (e.g. a saved template). Fills the YAML editor and shows the XML as-is. */
  loadXml(xml) {
    const yaml = this.xmlToYaml(xml); // throws on invalid XML, nothing is changed in that case
    this.xmlContent = xml, this.yamlContent = yaml;

    if (this.editor) {
      this.editor.setValue(yaml);
      clearTimeout(this._xmlTimer); // setValue schedules a regeneration; keep the original XML instead
    }
    this.showSourceXml();
  }

  /* ---------------------------- YAML -> XML ---------------------------- */
  /** Auto-quotes bare {table.field} values (they'd parse as YAML flow mappings), then parses. */
  yamlToObject(yaml) {
    // any value that is just "{...}" (no quotes inside), optionally followed by a comment
    const rgx = /^([ \t]*(?:-[ \t]+)?[^\s:#][^\n]*?:[ \t]+)(\{[^{}"'\n]*\})[ \t]*(?:#.*)?$/gm
    const safe = yaml.replace(/\r\n?/g, '\n').replace(rgx,'$1"$2"');
    return jsyaml.load(safe);
  }

  /** Safety net: a flow mapping like {"customers.id": null} is really the placeholder {customers.id} */
  placeholderOf(value) {
    if (value === null || typeof value !== 'object' || Array.isArray(value)) return null;
    const keys = Object.keys(value);
    if (keys.length !== 1 || value[keys[0]] !== null) return null;
    return /^[A-Za-z0-9_.-]+\.[A-Za-z0-9_]+$/.test(keys[0]) ? `{${keys[0]}}` : null;
  }

  xmlName = (key) => String(key).replace(/[^A-Za-z0-9_.-]/g, '_').replace(/^([^A-Za-z_])/, '_$1');

  escapeXml = (v) =>  String(v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

  nodeToXml(key, value, level) {
    const pad = '  '.repeat(level), tag = this.xmlName(key);

    // Arrays repeat the element once per item
    if (Array.isArray(value)) return value.map(v => this.nodeToXml(key, v, level)).join('\n');

    const placeholder = this.placeholderOf(value);
    if (placeholder) return `${pad}<${tag}>${this.escapeXml(placeholder)}</${tag}>`;

    if (value !== null && typeof value === 'object') {
      const inner = Object.entries(value).map(([k, v]) => this.nodeToXml(k, v, level + 1)).join('\n');
      return inner ? `${pad}<${tag}>\n${inner}\n${pad}</${tag}>` : `${pad}<${tag}></${tag}>`;
    }

    return `${pad}<${tag}>${value == null ? '' : this.escapeXml(value)}</${tag}>`;
  }

  yamlToXml(yaml) {
    const data = this.yamlToObject(yaml);
    if (data === null || typeof data !== 'object' || Array.isArray(data))
      throw new Error('The YAML root must be a mapping (key: value)');

    const e = Object.entries(data);
    const single = e.length === 1 && e[0][1] !== null && typeof e[0][1] === 'object' && !Array.isArray(e[0][1]);

    const body = single
      ? this.nodeToXml(e[0][0], e[0][1], 0) // single top key becomes the root
      : this.nodeToXml('DataMapping', data, 0); // otherwise wrap it

    return `<?xml version="1.0" encoding="UTF-8"?>\n${body}`;
  }

  /** silent = true while typing (half-written YAML is expected to be invalid) */
  logXml(silent = false) {
    const yaml = this.editor ? this.editor.getValue() : this.yamlContent;
    try {
      const xml = this.yamlToXml(yaml);
      console.log('[CustomMapping] Generated XML:\n' + xml);
      return xml;
    } catch (err) {
      if (!silent) console.warn('[CustomMapping] Invalid YAML:', err.message);
      return null;
    }
  }
}