import { BaseController } from "../../../../@still/component/super/service/BaseController.js";
import { AppTemplate } from "../../../../config/app-template.js";
import { DataQualityDeclaration } from "../quality/DataQualityDeclaration.js";

export class QualityDeclarationController extends BaseController {

  /** @type { DataQualityDeclaration } */ obj;

  targetDataset = 'public.orders';
  activeTab = 'sql';
  quarantineRecords = [];

  rules = []; //sampleRules;

  NO_COLUMN_TYPES = ['CUSTOM_SQL', 'ROW_COUNT'];
  DATASET_LEVEL_TYPES = ['FRESHNESS', 'ROW_COUNT'];
  LIVE_EVALUABLE_TYPES = ['NOT_NULL', 'UNIQUE', 'ACCEPTED_VALUES', 'VALUE_RANGE', 'REGEX_MATCH'];

   TABLE_COLUMNS_SAMPLE = {}; //sampleTables;

  async initComponent() {
    this.obj.container = document.getElementsByClassName(this.obj.cmpInternalId)[0];

    if (this.obj.container) {
      this.obj.targetDatasetInput = this.obj.$('#target-dataset');
      this.obj.rulesContainer = this.obj.$('#rules-container');
      this.obj.codeOutput = this.obj.$('#code-output');
      this.obj.sampleDataInput = this.obj.$('#sample-data-input');
      this.obj.sampleInputWrap = this.obj.$('#sample-input-wrap');
      this.obj.quarantineList = this.obj.$('#quarantine-list');
      this.obj.quarantineCountBadge = this.obj.$('#quarantine-count-badge');
    }

    this.bindEvents();
    this.renderRules();
    this.compileAll();
  }

  bindEvents() {
    if (this.obj.targetDatasetInput) {
      this.obj.targetDatasetInput.addEventListener('input', (e) => {
        this.targetDataset = e.target.value.trim();
        this.compileAll();
      });
      this.obj.targetDatasetInput.addEventListener('change', () => { this.renderRules(), this.compileAll(); });
    }
  }

  getColumnsForDataset(dataset) { return (this.obj.databaseSchema || {})[dataset] || []; }

  getSeverityBg(severity) {
    switch (severity) {
      case 'CRITICAL': return 'var(--badge-error-bg)';
      case 'ERROR': return '#FCEBEB';
      case 'WARN': return 'var(--badge-warn-bg)';
      default: return 'var(--badge-info-bg)';
    }
  }

  getSeverityTxt(severity) {
    switch (severity) {
      case 'CRITICAL': return 'var(--badge-error-txt)';
      case 'ERROR': return '#A32D2D';
      case 'WARN': return 'var(--badge-warn-txt)';
      default: return 'var(--badge-info-txt)';
    }
  }

  gateAction(severity) {
    switch (severity) {
      case 'CRITICAL': return 'HALT_AND_ROLLBACK';
      case 'ERROR': return 'FAIL_JOB';
      case 'WARN': return 'PIPELINE_ALERT';
      default: return 'LOG_ONLY';
    }
  }

  escSql(str) {
    return String(str == null ? '' : str).replace(/'/g, "''");
  }

  addRule = (e) => {
    if (e) e.preventDefault();
    const newId = 'dq_' + Math.floor(100 + Math.random() * 900);
    this.rules.push({ id: newId, type: 'NOT_NULL', column: '', severity: 'ERROR', params: {} });
    this.renderRules();
    this.compileAll();
  };

  removeRule(id) {
    this.rules = this.rules.filter(r => r.id !== id);
    this.renderRules();
    this.compileAll();
  }

  updateRule(id, key, value) {
    const rule = this.rules.find(r => r.id === id);
    if (rule) {
      rule[key] = value;
      if (key === 'type') rule.params = {};
      this.renderRules();
      this.compileAll();
    }
  }

  updateRuleParam(id, paramKey, value) {
    const rule = this.rules.find(r => r.id === id);
    if (rule) {
      rule.params[paramKey] = value;
      this.compileAll();
    }
  }

  setActiveTab(tabName, tabElement) {
    this.activeTab = tabName;
    const tabs = this.obj.$$('.tab-btn');
    tabs.forEach(t => t.classList.remove('active'));
    tabElement.classList.add('active');
    this.compileAll();
  }

  toggleSampleInput = () => {
    if (this.obj.sampleInputWrap) {
      const isHidden = this.obj.sampleInputWrap.style.display === 'none';
      this.obj.sampleInputWrap.style.display = isHidden ? 'flex' : 'none';
    }
  };

  renderRules() {
    if (!this.obj.rulesContainer) return;

    this.obj.rulesContainer.innerHTML = this.rules.map((rule, idx) => this.obj.parseEvents(`
      <div class="rule-card">
        <div class="rule-card-header">
          <div class="rule-card-title">
            <span>Rule #${idx + 1}</span>
            <span class="badge" style="background: ${this.getSeverityBg(rule.severity)}; color: ${this.getSeverityTxt(rule.severity)}">${rule.severity}</span>
          </div>
          <button type="button" class="btn btn-danger btn-sm" style="width:auto;" onclick="controller.removeRule('${rule.id}')" title="Delete Rule">✕ Remove</button>
        </div>

        <div class="rule-card-body">
          <div class="field-group">
            <label>Assertion Type</label>
            <select class="select-control" onchange="controller.updateRule('${rule.id}', 'type', this.value)">
              <option value="NOT_NULL" ${rule.type === 'NOT_NULL' ? 'selected' : ''}>NOT NULL</option>
              <option value="UNIQUE" ${rule.type === 'UNIQUE' ? 'selected' : ''}>UNIQUE / PRIMARY KEY</option>
              <option value="ACCEPTED_VALUES" ${rule.type === 'ACCEPTED_VALUES' ? 'selected' : ''}>ACCEPTED VALUES</option>
              <option value="VALUE_RANGE" ${rule.type === 'VALUE_RANGE' ? 'selected' : ''}>MIN / MAX RANGE</option>
              <option value="REGEX_MATCH" ${rule.type === 'REGEX_MATCH' ? 'selected' : ''}>REGEX / FORMAT MATCH</option>
              <option value="REFERENTIAL_INTEGRITY" ${rule.type === 'REFERENTIAL_INTEGRITY' ? 'selected' : ''}>REFERENTIAL INTEGRITY</option>
              <option value="FRESHNESS" ${rule.type === 'FRESHNESS' ? 'selected' : ''}>FRESHNESS / RECENCY</option>
              <option value="ROW_COUNT" ${rule.type === 'ROW_COUNT' ? 'selected' : ''}>ROW COUNT / VOLUME</option>
              <option value="CUSTOM_SQL" ${rule.type === 'CUSTOM_SQL' ? 'selected' : ''}>CUSTOM SQL ASSERTION</option>
            </select>
          </div>

          ${!this.NO_COLUMN_TYPES.includes(rule.type) ? `
            <div class="field-group">
              <label>${rule.type === 'FRESHNESS' ? 'Timestamp Column' : 'Target Column'}</label>
              ${this.renderColumnSelect(rule)}
            </div>
          ` : ''}

          <div class="field-group" ${this.NO_COLUMN_TYPES.includes(rule.type) ? 'style="grid-column: span 2;"' : ''}>
            <label>Severity Level</label>
            <select class="select-control" onchange="controller.updateRule('${rule.id}', 'severity', this.value)">
              <option value="INFO" ${rule.severity === 'INFO' ? 'selected' : ''}>INFO (Log only)</option>
              <option value="WARN" ${rule.severity === 'WARN' ? 'selected' : ''}>WARN (Pipeline Alert)</option>
              <option value="ERROR" ${rule.severity === 'ERROR' ? 'selected' : ''}>ERROR (Fail Job)</option>
              <option value="CRITICAL" ${rule.severity === 'CRITICAL' ? 'selected' : ''}>CRITICAL (Halt & Rollback)</option>
            </select>
          </div>
        </div>

        ${this.renderDynamicParams(rule)}
      </div>
    `)).join('');
  }

  renderColumnSelect(rule) {
    let columns = this.getColumnsForDataset(this.targetDataset);
    
    if (columns.length === 0) 
      return `<input type="text" class="input-control mono" value="${rule.column}" placeholder="column_name (unknown table — type manually)" oninput="controller.updateRule('${rule.id}', 'column', this.value)">`;

    const hasCurrent = rule.column && columns.includes(rule.column);
    const options = [`<option value="">— select column —</option>`].concat(columns.map(c => `<option value="${c}" ${rule.column === c ? 'selected' : ''}>${c}</option>`));

    if (rule.column && !hasCurrent) 
      options.push(`<option value="${rule.column}" selected>${rule.column} (not in ${this.targetDataset})</option>`);
    
    return `<select class="select-control" onchange="controller.updateRule('${rule.id}', 'column', this.value)">${options.join('')}</select>`;
  }

  renderDynamicParams(rule) {
    if (rule.type === 'ACCEPTED_VALUES') {
      return `
        <div class="rule-card-body full" style="border-top: 0.5px dashed var(--color-border-tertiary); padding-top: 8px;">
          <div class="field-group">
            <label>Allowed Values (Comma separated strings or numbers)</label>
            <input type="text" class="input-control mono" value="${rule.params.values || ''}" placeholder="'active', 'inactive'" oninput="controller.updateRuleParam('${rule.id}', 'values', this.value)">
          </div>
        </div>`;
    }
    if (rule.type === 'VALUE_RANGE') {
      return `
        <div class="rule-card-body two" style="border-top: 0.5px dashed var(--color-border-tertiary); padding-top: 8px;">
          <div class="field-group">
            <label>Minimum Threshold</label>
            <input type="text" class="input-control mono" value="${rule.params.min || ''}" placeholder="0" oninput="controller.updateRuleParam('${rule.id}', 'min', this.value)">
          </div>
          <div class="field-group">
            <label>Maximum Threshold</label>
            <input type="text" class="input-control mono" value="${rule.params.max || ''}" placeholder="1000" oninput="controller.updateRuleParam('${rule.id}', 'max', this.value)">
          </div>
        </div>`;
    }
    if (rule.type === 'REGEX_MATCH') {
      return `
        <div class="rule-card-body full" style="border-top: 0.5px dashed var(--color-border-tertiary); padding-top: 8px;">
          <div class="field-group">
            <label>Pattern (JS-compatible regex for live preview; POSIX/PCRE for SQL)</label>
            <input type="text" class="input-control mono" value="${rule.params.pattern || ''}" placeholder="^[A-Z0-9-]+$" oninput="controller.updateRuleParam('${rule.id}', 'pattern', this.value)">
          </div>
        </div>`;
    }
    if (rule.type === 'REFERENTIAL_INTEGRITY') {
      return `
        <div class="rule-card-body two" style="border-top: 0.5px dashed var(--color-border-tertiary); padding-top: 8px;">
          <div class="field-group">
            <label>Reference Table (schema.table)</label>
            <input type="text" class="input-control mono" value="${rule.params.ref_table || ''}" placeholder="public.customers" oninput="controller.updateRuleParam('${rule.id}', 'ref_table', this.value)">
          </div>
          <div class="field-group">
            <label>Reference Column</label>
            <input type="text" class="input-control mono" value="${rule.params.ref_column || ''}" placeholder="customer_id" oninput="controller.updateRuleParam('${rule.id}', 'ref_column', this.value)">
          </div>
        </div>`;
    }
    if (rule.type === 'FRESHNESS') {
      return `
        <div class="rule-card-body full" style="border-top: 0.5px dashed var(--color-border-tertiary); padding-top: 8px;">
          <div class="field-group">
            <label>Max Age Before Stale (hours)</label>
            <input type="text" class="input-control mono" value="${rule.params.max_age_hours || ''}" placeholder="24" oninput="controller.updateRuleParam('${rule.id}', 'max_age_hours', this.value)">
          </div>
        </div>`;
    }
    if (rule.type === 'ROW_COUNT') {
      return `
        <div class="rule-card-body two" style="border-top: 0.5px dashed var(--color-border-tertiary); padding-top: 8px;">
          <div class="field-group">
            <label>Minimum Expected Rows</label>
            <input type="text" class="input-control mono" value="${rule.params.min_rows || ''}" placeholder="1" oninput="controller.updateRuleParam('${rule.id}', 'min_rows', this.value)">
          </div>
          <div class="field-group">
            <label>Maximum Expected Rows (optional)</label>
            <input type="text" class="input-control mono" value="${rule.params.max_rows || ''}" placeholder="e.g. 5000000" oninput="controller.updateRuleParam('${rule.id}', 'max_rows', this.value)">
          </div>
        </div>`;
    }
    if (rule.type === 'CUSTOM_SQL') {
      return `
        <div class="rule-card-body full" style="border-top: 0.5px dashed var(--color-border-tertiary); padding-top: 8px;">
          <div class="field-group">
            <label>SQL Boolean Predicate Expression (Must evaluate to TRUE for valid rows)</label>
            <input type="text" class="input-control mono" value="${rule.params.sql || ''}" placeholder="column_a > column_b" oninput="controller.updateRuleParam('${rule.id}', 'sql', this.value)">
          </div>
        </div>`;
    }
    return '';
  }

  getFailuresSQL(rule) {
    const col = rule.column || 'column';
    const dataset = this.targetDataset;
    switch (rule.type) {
      case 'NOT_NULL':
        return `SELECT *\n  FROM ${dataset}\n  WHERE ${col} IS NULL`;
      case 'UNIQUE':
        return `SELECT *\n  FROM ${dataset} d\n  WHERE EXISTS (\n    SELECT 1 FROM ${dataset} d2\n    WHERE d2.${col} = d.${col}\n    GROUP BY d2.${col} HAVING COUNT(*) > 1\n  )`;
      case 'ACCEPTED_VALUES':
        return `SELECT *\n  FROM ${dataset}\n  WHERE ${col} NOT IN (${rule.params.values || 'NULL'})`;
      case 'VALUE_RANGE': {
        const conds = [];
        if (rule.params.min !== undefined && rule.params.min !== '') conds.push(`${col} < ${rule.params.min}`);
        if (rule.params.max !== undefined && rule.params.max !== '') conds.push(`${col} > ${rule.params.max}`);
        return `SELECT *\n  FROM ${dataset}\n  WHERE ${conds.join(' OR ') || 'FALSE'}`;
      }
      case 'REGEX_MATCH':
        return `SELECT *\n  FROM ${dataset}\n  WHERE ${col} IS NOT NULL\n    AND ${col} !~ '${this.escSql(rule.params.pattern || '.*')}'  -- Postgres regex; swap for your engine's operator`;
      case 'REFERENTIAL_INTEGRITY': {
        const refTable = rule.params.ref_table || 'ref_table';
        const refCol = rule.params.ref_column || 'id';
        return `SELECT d.*\n  FROM ${dataset} d\n  WHERE d.${col} IS NOT NULL\n    AND NOT EXISTS (\n      SELECT 1 FROM ${refTable} r WHERE r.${refCol} = d.${col}\n    )`;
      }
      case 'FRESHNESS': {
        const hrs = rule.params.max_age_hours || '24';
        return `SELECT MAX(${col}) AS most_recent_value, NOW() - MAX(${col}) AS staleness\n  FROM ${dataset}\n  HAVING MAX(${col}) < NOW() - INTERVAL '${hrs} hours'`;
      }
      case 'ROW_COUNT': {
        const min = rule.params.min_rows, max = rule.params.max_rows, conds = [];
        if (min !== undefined && min !== '') conds.push(`COUNT(*) < ${min}`);
        if (max !== undefined && max !== '') conds.push(`COUNT(*) > ${max}`);
        return `SELECT COUNT(*) AS actual_row_count\n  FROM ${dataset}\n  HAVING ${conds.join(' OR ') || 'FALSE'}`;
      }
      case 'CUSTOM_SQL':
      default:
        return `SELECT *\n  FROM ${dataset}\n  WHERE NOT (${rule.params.sql || 'TRUE'})`;
    }
  }

  getMessageExpr(rule, countExpr) {
    const col = rule.column || 'expression';
    const dataset = this.targetDataset;
    switch (rule.type) {
      case 'NOT_NULL':
        return `'${col} contains ' || ${countExpr} || ' NULL value(s) in ${dataset} (expected: NOT NULL)'`;
      case 'UNIQUE':
        return `${countExpr} || ' row(s) share a duplicated value on ${col} in ${dataset} (expected: UNIQUE)'`;
      case 'ACCEPTED_VALUES':
        return `${countExpr} || ' row(s) in ${dataset} have ${col} outside allowed values (${this.escSql(rule.params.values || '')})'`;
      case 'VALUE_RANGE':
        return `${countExpr} || ' row(s) in ${dataset} have ${col} outside range [${rule.params.min || '-inf'}, ${rule.params.max || '+inf'}]'`;
      case 'REGEX_MATCH':
        return `${countExpr} || ' row(s) in ${dataset} have ${col} not matching pattern /${this.escSql(rule.params.pattern || '')}/'`;
      case 'REFERENTIAL_INTEGRITY':
        return `${countExpr} || ' row(s) in ${dataset} have ${col} with no matching ${rule.params.ref_column || 'id'} in ${rule.params.ref_table || 'ref_table'}'`;
      case 'FRESHNESS':
        return `'${dataset} has not been refreshed within ${rule.params.max_age_hours || '24'}h based on ${col}'`;
      case 'ROW_COUNT':
        return `'${dataset} row count is outside expected bounds [${rule.params.min_rows || '0'}, ${rule.params.max_rows || '\\u221e'}]'`;
      case 'CUSTOM_SQL':
      default:
        return `${countExpr} || ' row(s) in ${dataset} failed custom predicate: ${this.escSql(rule.params.sql || '')}'`;
    }
  }

  compileAll() {
    if (!this.obj.codeOutput) return;

    if (this.activeTab === 'json')
      return this.obj.codeOutput.textContent = this.compileJSON();
    if (this.activeTab === 'gate') 
      return this.obj.codeOutput.textContent = this.compileGate();
    if (this.activeTab === 'quarantine-sql') 
      return this.obj.codeOutput.textContent = this.compileQuarantineSQL();

    this.obj.codeOutput.textContent = this.compileSQL();
  }

  compileSQL() {
    const lines = [
      `-- ============================================================`,
      `-- Data Quality Audit Suite for: ${this.targetDataset}`,
      `-- Rules: ${this.rules.length}  |  Generated: ${new Date().toISOString()}`,
      `-- ============================================================\n`
    ];

    this.rules.forEach((rule, idx) => {
      const alias = `${rule.id}_failures`;
      const failuresSQL = this.getFailuresSQL(rule);
      const messageExpr = this.getMessageExpr(rule, `(SELECT COUNT(*) FROM ${alias})`);
      const target = rule.column || '(dataset-level)';

      lines.push(`-- Rule #${idx + 1} [${rule.severity}] ${rule.type} on '${target}'  (${rule.id})`);
      lines.push(`WITH ${alias} AS (`);
      lines.push(`  ${failuresSQL.split('\n').join('\n  ')}`);
      lines.push(`)`);
      lines.push(`SELECT`);
      lines.push(`  '${rule.id}' AS rule_id, '${rule.severity}' AS severity, '${rule.type}' AS assertion_type,`);
      lines.push(`  '${target}' AS target, '${this.gateAction(rule.severity)}' AS action_if_failed,`);
      lines.push(`  (SELECT COUNT(*) FROM ${alias}) AS invalid_records,`);
      lines.push(`  CASE WHEN (SELECT COUNT(*) FROM ${alias}) > 0 THEN ${messageExpr} ELSE 'OK — no violations found' END AS message;\n`);
    });

    return lines.join('\n');
  }

  compileGate() {
    const lines = [
      `-- ============================================================`,
      `-- Orchestration Gate for: ${this.targetDataset}`,
      `-- ============================================================\n`,
      `WITH rule_results AS (`
    ];

    const unionBlocks = this.rules.map((rule) => {
      const alias = `${rule.id}_f`;
      const failuresSQL = this.getFailuresSQL(rule);
      const target = rule.column || '(dataset-level)';
      const messageExpr = this.getMessageExpr(rule, `(SELECT COUNT(*) FROM ${alias})`);
      return (
        `  SELECT\n` +
        `    '${rule.id}' AS rule_id, '${rule.severity}' AS severity, '${rule.type}' AS assertion_type,\n` +
        `    '${target}' AS target, '${this.gateAction(rule.severity)}' AS action_if_failed,\n` +
        `    (SELECT COUNT(*) FROM (${failuresSQL.split('\n').join(' ')}) ${alias}) AS invalid_records,\n` +
        `    CASE WHEN (SELECT COUNT(*) FROM (${failuresSQL.split('\n').join(' ')}) ${alias}) > 0 THEN ${messageExpr} ELSE 'OK' END AS message`
      );
    });

    lines.push(unionBlocks.join('\n  UNION ALL\n'));
    lines.push(`)\nSELECT * FROM rule_results WHERE invalid_records > 0;`);
    return lines.join('\n');
  }

  compileQuarantineSQL() {
    const lines = [
      `-- Quarantine capture for: ${this.targetDataset}\n`,
      `CREATE TABLE IF NOT EXISTS dq_quarantine (`,
      `  quarantine_id BIGSERIAL PRIMARY KEY, rule_id TEXT NOT NULL, severity TEXT NOT NULL,`,
      `  assertion_type TEXT NOT NULL, target TEXT, dataset TEXT NOT NULL, message TEXT NOT NULL,`,
      `  record_json JSONB NOT NULL, captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
      `);\n`
    ];

    const rowLevelRules = this.rules.filter(r => !this.DATASET_LEVEL_TYPES.includes(r.type));

    rowLevelRules.forEach((rule, idx) => {
      const failuresSQL = this.getFailuresSQL(rule), target = rule.column || '(dataset-level)';
      const msg = this.getMessageExpr(rule, `(SELECT COUNT(*) FROM t)`);

      lines.push(`-- Quarantine Rule #${idx + 1} (${rule.id})`);
      lines.push(`INSERT INTO dq_quarantine (rule_id, severity, assertion_type, target, dataset, message, record_json)`);
      lines.push(`SELECT '${rule.id}', '${rule.severity}', '${rule.type}', '${target}', '${this.targetDataset}', ${msg}, to_jsonb(t)`);
      lines.push(`FROM (${failuresSQL.split('\n').join('\n  ')}) t;\n`);
    });

    return lines.join('\n');
  }

  compileJSON() {
    return JSON.stringify({
      dataset: this.targetDataset, version: "3.0", generated_at: new Date().toISOString(),
      rule_count: this.rules.length, assertions: this.rules, live_preview_quarantined_records: this.quarantineRecords
    }, null, 2);
  }

  evaluateSampleData = () => {
    if (!this.obj.sampleDataInput) return;

    const raw = this.obj.sampleDataInput.value.trim();
    let rows;
    try {
      rows = JSON.parse(raw);
      if (!Array.isArray(rows)) throw new Error('not an array');
    } catch (e) {
      this.obj.quarantineList.innerHTML = `<div class="quarantine-empty">Couldn't parse that as a JSON array of row objects. Example: [{"order_id": 1, "order_status": "completed"}]</div>`;
      this.obj.quarantineCountBadge.style.display = 'none';
      return this.quarantineRecords = [];
    }

    this.quarantineRecords = [];
    const skippedTypes = new Set();

    this.rules.forEach(rule => {
      if (!this.LIVE_EVALUABLE_TYPES.includes(rule.type)) 
        return skippedTypes.add(rule.type);
      
      const failingRows = this.evaluateRuleAgainstRows(rule, rows);
      if (!failingRows) 
        return skippedTypes.add(rule.type);

      failingRows.forEach((row, i) => {
        this.quarantineRecords.push({
          quarantine_id: `${rule.id}_${i}`, rule_id: rule.id, severity: rule.severity, assertion_type: rule.type, target: rule.column || '(dataset-level)',
          dataset: this.targetDataset, message: this.buildLiveMessage(rule, row), captured_at: new Date().toISOString(), record: row
        });
      });
    });

    this.renderQuarantineList(skippedTypes);
    if (this.activeTab === 'json') this.compileAll();
  };

  evaluateRuleAgainstRows(rule, rows) {
    const col = rule.column;
    switch (rule.type) {
      case 'NOT_NULL':
        return rows.filter(r => r[col] === null || r[col] === undefined || r[col] === '');
      case 'UNIQUE': {
        const counts = {};
        rows.forEach(r => { const v = r[col]; counts[v] = (counts[v] || 0) + 1; });
        return rows.filter(r => counts[r[col]] > 1);
      }
      case 'ACCEPTED_VALUES': {
        const allowed = (rule.params.values || '').split(',').map(v => v.trim().replace(/^'(.*)'$/, '$1').replace(/^"(.*)"$/, '$1'));
        return rows.filter(r => !allowed.includes(String(r[col])));
      }
      case 'VALUE_RANGE': {
        const min = (rule.params.min !== undefined && rule.params.min !== '') ? parseFloat(rule.params.min) : -Infinity;
        const max = (rule.params.max !== undefined && rule.params.max !== '') ? parseFloat(rule.params.max) : Infinity;
        return rows.filter(r => { const v = parseFloat(r[col]); return isNaN(v) || v < min || v > max; });
      }
      case 'REGEX_MATCH': {
        let re;
        try { re = new RegExp(rule.params.pattern || '.*'); } catch (e) { return []; }
        return rows.filter(r => r[col] !== null && r[col] !== undefined && !re.test(String(r[col])));
      }
      default:
        return null;
    }
  }

  buildLiveMessage(rule, row) {
    const col = rule.column;
    const val = row ? row[col] : undefined;
    switch (rule.type) {
      case 'NOT_NULL': return `${col} is null/empty (expected: NOT NULL)`;
      case 'UNIQUE': return `${col} = ${JSON.stringify(val)} is duplicated elsewhere in the sample`;
      case 'ACCEPTED_VALUES': return `${col} = ${JSON.stringify(val)} is not in the allowed values`;
      case 'VALUE_RANGE': return `${col} = ${JSON.stringify(val)} is outside [${rule.params.min || '-inf'}, ${rule.params.max || '+inf'}]`;
      case 'REGEX_MATCH': return `${col} = ${JSON.stringify(val)} does not match /${rule.params.pattern || ''}/`;
      default: return `${rule.type} violation on ${col || '(dataset)'}`;
    }
  }

  renderQuarantineList(skippedTypes) {
    if (!this.obj.quarantineList) return;

    if (this.quarantineRecords.length === 0) {
      this.obj.quarantineList.innerHTML = `<div class="quarantine-empty">No violations found in this sample for the browser-evaluable rule types. ✅</div>`;
      this.obj.quarantineCountBadge.style.display = 'none';
    } else {
      this.obj.quarantineCountBadge.style.display = 'inline-flex';
      this.obj.quarantineCountBadge.textContent = `${this.quarantineRecords.length} record${this.quarantineRecords.length === 1 ? '' : 's'} quarantined`;

      const sorted = [...this.quarantineRecords].sort((a, b) => {
        const rank = s => ({ CRITICAL: 0, ERROR: 1, WARN: 2, INFO: 3 }[s] ?? 4);
        return rank(a.severity) - rank(b.severity);
      });

      this.obj.quarantineList.innerHTML = sorted.map((rec, idx) => this.obj.parseEvents(`
        <div class="q-card" id="qcard-${idx}">
          <div class="q-card-header" onclick="controller.toggleQCard(${idx})">
            <span class="chev">▶</span>
            <span class="badge" style="background: ${this.getSeverityBg(rec.severity)}; color: ${this.getSeverityTxt(rec.severity)}">${rec.severity}</span>
            <span class="q-card-rule">${rec.rule_id}</span><span class="q-card-msg">${this.escapeHtml(rec.message)}</span>
          </div>
          <div class="q-card-body"><pre>${this.escapeHtml(JSON.stringify(rec, null, 2))}</pre></div>
        </div>
      `)).join('');
    }

    const skipped = skippedTypes ? Array.from(skippedTypes) : [];
    if (skipped.length) 
      this.obj.quarantineList.innerHTML += `<div class="quarantine-note">Not evaluated in-browser (need a real warehouse): ${skipped.join(', ')}. These still generate correctly in the Quarantine SQL / Orchestration Gate tabs.</div>`;
  }

  toggleQCard(idx) {
    const card = this.obj.$(`#qcard-${idx}`);
    if (card) card.classList.toggle('expanded');
  }

  escapeHtml(str) {
    return String(str)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  copyOutput = () => {
    navigator.clipboard.writeText(this.obj.codeOutput.textContent);
    AppTemplate.toast.success('DQ Specification copied to clipboard!')
  };

  copyQuarantineJSON = () => {
    if (this.quarantineRecords.length === 0) 
      return alert('No quarantined records yet — paste sample data and click Evaluate first.');
    navigator.clipboard.writeText(JSON.stringify(this.quarantineRecords, null, 2));
    AppTemplate.toast.success(`Copied ${this.quarantineRecords.length} quarantine record(s) as JSON.`);
  };
}