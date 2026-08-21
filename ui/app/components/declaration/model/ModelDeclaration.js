import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { Assets } from "../../../../@still/util/componentUtil.js";
import { switchActiveTab } from "../../../util/tabs.js";

const DEFAULT_YAML = `version: 1
filters:
  - or:
      - "orders.order_status = 'completed'"
      - "orders.order_status = 'shipped'"
  - "regions.region_name = 'North America'"

tables:
  - table: public.orders as orders
    dimensions:
      - name: id as order_id
      - name: "id + 3"
      - name: "(id + 3) as id_plus_3"
      - name: customer_id
      - name: status as order_status
    measures:
      - name: amount as total_revenue
        agg: SUM
      - name: discount_amount as total_discount
        agg: SUM
      - name: id as distinct_order_count
        agg: COUNT_DISTINCT

      - name: net_revenue
        sql: "orders.total_revenue - orders.total_discount"

      - name: avg_order_value
        sql: "orders.total_revenue / NULLIF(orders.distinct_order_count, 0)"

      - name: "SUM(orders.amount * 0.10) as estimated_tax"

  - table: public.customers as customers
    dimensions:
      - id as customer_id
      - full_name as customer_name
      - region_id

  - table: public.regions as regions
    dimensions:
      - id as region_id
      - name as region_name

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

export class ModelDeclaration extends ViewComponent {

  isPublic = false;

  /** @Prop @type { HTMLElement } */ yamlInput;

  /** @Prop @type { HTMLElement } */ sqlOutput;
  
  /** @Prop @type { HTMLElement } */ errorBox;

  /** @Prop @type { HTMLElement } */ container;

  async stBeforeInit(){
  	await Assets.import({ path: '/app/components/pipeline/styles/shared.css', type: 'css' });
  }

  stAfterInit() {
    this.container = document.getElementsByClassName(this.cmpInternalId)[0];

    if (this.container) {
      this.yamlInput = this.container.querySelector('#yaml-input');
      this.sqlOutput = this.container.querySelector('#sql-output');
      this.errorBox = this.container.querySelector('#error-box');
    }

    this.bindEvents();
    this.loadSampleYAML();
  }

  /**
   * Attaches event listeners scoped to the component instance shell
   */
  bindEvents() {
    if (this.yamlInput) {
      this.yamlInput.addEventListener('input', () => this.compileYAMLToSQL());
    }

    const resetBtn = this.container ? this.container.querySelector('#btn-reset, button[onclick*="loadSampleYAML"]') : null;
    if (resetBtn) {
      resetBtn.onclick = (e) => {
        e.preventDefault();
        this.loadSampleYAML();
      };
    }

    const copyBtn = this.container ? this.container.querySelector('#btn-copy, button[onclick*="copySQL"]') : null;
    if (copyBtn) {
      copyBtn.onclick = (e) => {
        e.preventDefault();
        this.copySQL();
      };
    }
  }

  cleanValue(val) {
    if (!val) return '';
    val = val.trim();
    if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
      val = val.slice(1, -1).trim();
    }
    return val;
  }

  sanitizeAlias(rawStr) {
    if (!rawStr) return 'col';
    return rawStr.trim()
      .toLowerCase()
      .replace(/[^a-z0-9_]+/g, '_')
      .replace(/^_+|_+$/g, '')
      || 'col';
  }

  validateNoTrailingColon(rawStr, contextName) {
    if (!rawStr || typeof rawStr !== 'string') return;
    const trimmed = rawStr.trim();
    if (trimmed.endsWith(':')) {
      throw new Error(`Syntax Error in ${contextName}: Unexpected trailing colon in <code>"${trimmed}"</code>`);
    }
  }

  parseAliasString(str, contextName) {
    if (!str || typeof str !== 'string') return { sql: '', alias: '' };
    
    this.validateNoTrailingColon(str, contextName);
    const cleanStr = this.cleanValue(str);
    
    const parts = cleanStr.split(/\s+as\s+|\s+AS\s+/);
    if (parts.length > 1) {
      const rawSql = this.cleanValue(parts[0]);
      const rawAlias = this.cleanValue(parts[1]);
      this.validateNoTrailingColon(rawAlias, contextName);
      return { sql: rawSql, alias: this.sanitizeAlias(rawAlias) };
    }
    
    return { sql: cleanStr, alias: this.sanitizeAlias(cleanStr) };
  }

  qualifyDimensionExpr(expr, tableAlias) {
    const trimmed = expr.trim();
    
    if (/^[a-zA-Z0-9_]+$/.test(trimmed)) {
      return `${tableAlias}.${trimmed}`;
    }
    
    if (/^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$/.test(trimmed)) {
      return trimmed;
    }

    const sqlKeywords = new Set(['AND', 'OR', 'NOT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'NULL', 'DATE_TRUNC', 'CAST', 'AS', 'COALESCE', 'NULLIF', 'DAY', 'MONTH', 'YEAR']);

    return trimmed.replace(/\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g, (match, offset, string) => {
      if (sqlKeywords.has(match.toUpperCase()) || !isNaN(match)) return match;
      const prevChar = string[offset - 1];
      if (prevChar === '.') return match;
      return `${tableAlias}.${match}`;
    });
  }

  parseSimpleYAML(yamlText) {
    const lines = yamlText.split('\n');
    let root = {};
    let stack = [{ indent: -1, obj: root }];

    for (let i = 0; i < lines.length; i++) {
      let rawLine = lines[i];
      if (!rawLine.trim() || rawLine.trim().startsWith('#')) continue;

      const indent = rawLine.search(/\S/);
      const line = rawLine.trim();

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

          const k = val.substring(0, firstColonIdx).trim();
          const v = val.substring(firstColonIdx + 1).trim();

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
            stack.push({ indent: indent, obj: itemObj });
            stack.push({ indent: indent + 1, obj: itemObj[k] });
          }
        } else {
          if (Array.isArray(parent)) {
            parent.push(this.cleanValue(val));
          }
        }
      } else if (line.includes(':')) {
        const firstColonIdx = line.indexOf(':');
        const key = line.substring(0, firstColonIdx).trim();
        const val = line.substring(firstColonIdx + 1).trim();

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
        } else {
          parent[key] = this.cleanValue(val);
        }
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
    if (this.errorBox) this.errorBox.style.display = 'none';

    try {
      if (!this.yamlInput) return;
      const yamlText = this.yamlInput.value;
      const model = this.parseSimpleYAML(yamlText);

      if (!model.tables || model.tables.length === 0) {
        if (this.sqlOutput) this.sqlOutput.textContent = '-- Define at least one table in YAML to compile SQL.';
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

        const baseMeasures = [];
        const calculatedMeasures = [];

        (t.measures || []).forEach(m => {
          let rawStr = typeof m === 'string' ? m : (m.name || m.sql || '');
          let agg = typeof m === 'object' ? this.cleanValue(m.agg || '') : '';
          let explicitSql = typeof m === 'object' ? this.cleanValue(m.sql || '') : '';

          const parsed = this.parseAliasString(rawStr, `measure in '${parsedTable.alias}'`);

          const isCalculated = explicitSql || agg.toUpperCase() === 'CALCULATED' || 
            parsed.sql.includes('/') || parsed.sql.includes('*') || parsed.sql.includes('+') || parsed.sql.includes('-');

          if (isCalculated) {
            calculatedMeasures.push({
              alias: parsed.alias || parsed.sql,
              formula: explicitSql || parsed.sql
            });
          } else {
            baseMeasures.push({
              alias: parsed.alias,
              sqlColumn: parsed.sql,
              agg: agg || 'SUM'
            });
          }
        });

        return {
          alias: parsedTable.alias,
          sqlTable: parsedTable.sql,
          dimensions,
          baseMeasures,
          calculatedMeasures
        };
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
          if (normalizedTables.some(t => t.alias === tAlias)) {
            validateFieldRef(tAlias, fName, contextLocation);
          }
        }
      };

      const expandCalculatedFormula = (rawFormula) => {
        let expanded = rawFormula;
        const matches = [...rawFormula.matchAll(/([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)/g)];

        for (const match of matches) {
          const [fullRef, tAlias, fName] = match;
          const targetTable = normalizedTables.find(t => t.alias === tAlias);

          if (targetTable) {
            const baseMeas = targetTable.baseMeasures.find(m => m.alias === fName || m.sqlColumn === fName);
            if (baseMeas) {
              const baseExpr = this.buildBaseMeasureSQL(tAlias, baseMeas.sqlColumn, baseMeas.agg);
              expanded = expanded.replaceAll(fullRef, baseExpr);
            } else {
              expanded = expanded.replaceAll(fullRef, `${tAlias}.${fName}`);
            }
          }
        }
        return expanded;
      };

      const compileFilterNode = (node, contextLoc = 'filters clause') => {
        if (!node) return '';

        if (typeof node === 'string') {
          this.validateFilterSyntax(node);
          validateSqlExpression(node, `${contextLoc} ("${node}")`);
          return node;
        }

        if (Array.isArray(node)) {
          const compiledParts = node
            .map((item, idx) => compileFilterNode(item, `${contextLoc} item #${idx + 1}`))
            .filter(Boolean);
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
            const val = node[key];
            const op = lowerKey.toUpperCase();
            const items = Array.isArray(val) ? val : [val];

            const compiledSub = items
              .map((sub, i) => compileFilterNode(sub, `${contextLoc} -> ${op}[${i}]`))
              .filter(Boolean);

            if (compiledSub.length === 0) return '';
            if (compiledSub.length === 1) return compiledSub[0];

            return `(${compiledSub.join(` ${op} `)})`;
          } else {
            throw new Error(`Invalid logic block '${key}' in filter. Expected 'or' or 'and'.`);
          }
        }

        return '';
      };

      (model.relationships || []).forEach(rel => {
        if (rel.sql_on) {
          this.validateNoTrailingColon(rel.sql_on, `relationship '${rel.name || 'unnamed'}' (sql_on)`);
          validateSqlExpression(rel.sql_on, `relationship '${rel.name || 'unnamed'}' (sql_on)`);
        }
      });

      const compiledWhereClause = compileFilterNode(model.filters);

      const selectClauses = [];
      const groupByClauses = [];
      let hasMeasures = false;

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

      const joinedAliases = new Set([baseTableAlias]);
      const unjoinedTables = normalizedTables.slice(1);
      const joinClauses = [];

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

      if (joinClauses.length > 0) {
        sql += `\n${joinClauses.join('\n')}`;
      }

      if (compiledWhereClause) {
        sql += `\nWHERE\n  ${compiledWhereClause}`;
      }

      if (hasMeasures && groupByClauses.length > 0) {
        sql += `\nGROUP BY\n${groupByClauses.join(',\n')}`;
      }

      if (this.sqlOutput) {
        this.sqlOutput.textContent = sql + ';';
      }

    } catch (err) {
      if (this.errorBox) {
        this.errorBox.innerHTML = `⚠️ <b>YAML Compiler Error:</b> ${err.message}`;
        this.errorBox.style.display = 'block';
      }
    }
  }

  loadSampleYAML() {
    if (this.yamlInput) {
      this.yamlInput.value = DEFAULT_YAML;
      this.compileYAMLToSQL();
    }
  }

  copySQL() {
    if (this.sqlOutput) {
      const text = this.sqlOutput.textContent;
      navigator.clipboard.writeText(text);
      alert('SQL copied to clipboard!');
    }
  }

  switchTab(el){ switchActiveTab(this, null, el) }

	/** @returns { HTMLElement } */ $ = (ref) => this.container.querySelector(ref);
	/** @returns { HTMLElement } */ $$ = (ref) => this.container.querySelectorAll(ref);

}