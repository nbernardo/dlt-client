import { BaseController } from "../../../../@still/component/super/service/BaseController.js";
import { Assets } from "../../../../@still/util/componentUtil.js";
import { Grid } from "../bi/grid/Grid.js";
import { DatabaseDiagram } from "../diagram/DatabaseDiagram.js";
import { BIService } from "../services/BIService.js";
import { PipelinePlanPayload, PipelinePlanService } from "../services/PipelinePlanService.js";
import { BIController } from "./BIController.js";
import { BIController2 } from "./BIController2.js";
import { DataQualityController as DQController } from "./DataQualityController.js";

export class DBDiagramController extends BaseController {
    
    /** @type { DatabaseDiagram } */ obj;

    editor;
    selectedConnection;
    selectedConnectionHost;
    selectedConnectionEngine;
    selectedDatabase;
    selectedExistingPpline;
    plannerMode = 'regular';
    selectedPlanId = null;

    /** @type { Map<string, number> } */ selectedTablesMap = new Map();
    relationRegistry = new Map(); 
    /** @type { Map<string, object> } */ pipelineTables = new Map();
    /** @type { Map<string, object> } */ pipelineTableFields = new Map();
    pipelineName;

    /** @returns { DBDiagramController } */
    static fromContext = () => DBDiagramController.get();

    /** @type { Grid } */ datagridInstance;

    handleTableSelect(tableName) {
        if (this.selectedTablesMap.has(tableName)) {
            this.selectedTablesMap.delete(tableName);
            const remaining = Array.from(this.selectedTablesMap.keys());
            this.selectedTablesMap.clear();
            remaining.forEach((name, i) => this.selectedTablesMap.set(name, i + 1));
        } else {
            const nextOrder = this.selectedTablesMap.size + 1;
            this.selectedTablesMap.set(tableName, nextOrder);
        }
    }

    isTableUnrelated(tableName) {
        if (this.selectedTablesMap.size <= 1) return false;
        const selected = Array.from(this.selectedTablesMap.keys()), relations = Array.from(this.relationRegistry.values());

        return !relations.some(rel => 
            (rel.sourceTable === tableName && selected.includes(rel.targetTable) && rel.targetTable !== tableName) || 
            (rel.targetTable === tableName && selected.includes(rel.sourceTable) && rel.sourceTable !== tableName)
        );
    }

    /** @param {Array} rows */
    compileRelations(rows) {
        rows.forEach(row => {
            const [_, sCol, sTable, tCol, tTable] = row;
            const relKey = `${sTable}.${sCol}->${tTable}.${tCol}`;
            if (!this.relationRegistry.has(relKey)) {
                this.relationRegistry.set(relKey, { sourceTable: sTable, sourceColumn: sCol, targetTable: tTable, targetColumn: tCol });
            }
        });
    }

    bindToolbar() {
        const btnDiagram = this.obj.container.querySelector('#btnDiagram');
        const btnSQL = this.obj.container.querySelector('#btnSQL');
        if (btnDiagram) btnDiagram.onclick = () => this.toggleView('diagram');
        if (btnSQL) btnSQL.onclick = () => this.toggleView('sql');
    }

    toggleView(view) {
        const diagramEl = this.obj.container.querySelector('#mountNode');
        const btnDiagram = this.obj.container.querySelector('#btnDiagram');
        const btnSQL = this.obj.container.querySelector('#btnSQL');

        if(btnDiagram.classList.contains('active') && view === 'diagram') return;

        if (view === 'diagram') {
            this.obj.showDiagram = true;
            btnDiagram.classList.add('active');
            btnSQL.classList.remove('active');
            if (this.obj.graph) this.obj.graph.changeSize(diagramEl.scrollWidth, diagramEl.scrollHeight);
        } else {
            this.obj.showDiagram = false;
            btnDiagram.classList.remove('active');
            btnSQL.classList.add('active');
            this.syncSqlEditor();
        }
    }

    parseFieldMap = (f, returnModule = false, tableNameOnly = false) => {
        let isArray = Array.isArray(f), selectedFields = null;
        if(isArray) { selectedFields = f[1], f = f[0] };
        let field = f, fieldPath = f.split('.'), pathSize = tableNameOnly === true ? 1 : 2;
        if(fieldPath.length > pathSize) field = fieldPath.slice(1).join('.');

        if(isArray) return [field, selectedFields];
        return returnModule === true ? [fieldPath.length > pathSize ? fieldPath[0] : null, field] : field;
    }

    syncSqlCount = 0;
    syncSqlEditor() {
        const selectedEntries = Array.from(this.selectedTablesMap.entries());
        if (selectedEntries.length === 0) 
            return this.editor.setValue(this.editorSelectedCode || `-- Write your query here. \n-- Or Add/Select tables in the diagram to the query.`);

        const activeTables = new Set();
        selectedEntries.forEach(([name]) => {
            const hasFields = Array.from(this.selectedFieldsSet).map(this.parseFieldMap).some(f => f.startsWith(`${name}.`));
            if (hasFields || this.selectedTablesMap.has(name)) 
                activeTables.add(name);
        });

        const [baseTable] = selectedEntries[0];
        const processed = new Set([baseTable]);
        const joins = [], relations = Array.from(this.relationRegistry.values());

        selectedEntries.slice(1).forEach(([currentTable]) => {
            if (!activeTables.has(currentTable)) return;

            const connection = relations.find(rel => 
                (rel.sourceTable === currentTable && processed.has(rel.targetTable)) || 
                (rel.targetTable === currentTable && processed.has(rel.sourceTable))
            );

            if (connection) {
                const isSource = connection.sourceTable === currentTable;
                const lTab = isSource ? connection.targetTable : connection.sourceTable;
                const lCol = isSource ? connection.targetColumn : connection.sourceColumn;
                const rCol = isSource ? connection.sourceColumn : connection.targetColumn;

                joins.push(`LEFT JOIN ${currentTable} ON ${currentTable}.${rCol} = ${lTab}.${lCol}`);
                processed.add(currentTable);
            }
        });

        let selectClause = "*";
        const validFields = Array.from(this.selectedFieldsSet)
            .map(fPath => {
                const parsedPath = this.parseFieldMap(fPath);                 
                const rule = DQController.fromContext().columnQualityRules.get(parsedPath.split(' ')[0]);
                
                if (rule) {
                    const [tbl, field] = parsedPath.split('.');
                    const cleanField = field.split(' ')[0];

                    if (rule === 'TRIM') return `TRIM(${tbl}.${cleanField}) AS ${cleanField}`;
                    if (rule === 'UPPER') return `UPPER(${tbl}.${cleanField}) AS ${cleanField}`;
                    if (rule === 'LOWER') return `LOWER(${tbl}.${cleanField}) AS ${cleanField}`;
                    if (rule === 'COALESCE') return `COALESCE(${tbl}.${cleanField}, '') AS ${cleanField}`;
                }
                return parsedPath;
            })
            .filter(f => processed.has(f.includes(' AS ') ? f.split(' AS ')[0].split('.')[0].split('(')[1] : f.split('.')[0]));
        
        if (validFields.length > 0)
            selectClause = validFields.map(itm => itm.includes(' AS ') ? itm :  itm.split(' ')[0]).join(',\n       ');
        if(this.syncSqlCount > 0 && this.editorSelectedCode !== null) this.editorSelectedCode = null;
        
        const sqlCode = this.editorSelectedCode
                || [`-- Auto-generated Business Query`,`SELECT ${selectClause}`,`FROM ${baseTable}`,joins.length > 0 ? `    ${joins.join('\n    ')}` : '','LIMIT 100;'].filter(v => v.trim()).join('\n');
        this.syncSqlCount++;
        this.editor.setValue(sqlCode);
    }

    static initCustomDBNode() {
        G6.registerNode('db-table', {
            draw(cfg, group) {
                const { label = '', isRoot, isExternal, isSelected, orderNumber, selectIconColor, level, relationLabel, depth } = cfg;
                const fontSize = 8, height = 20;
                const width = DBDiagramController.calculateTextWidth(label, `${fontSize}px Arial`) + 65;

                let fill = cfg.level === 1 ? '#e6f7ff' : '#f0f5ff';
                let stroke = isSelected ? '#1890ff' : (cfg.level === 1 ? '#91d5ff' : '#adc6ff');
                let textColor = isRoot ? '#ffffff' : '#000000';
                let lineWidth = isSelected ? 2 : 1;

                if (isRoot) { fill = '#2c3e50'; stroke = '#002329'; } 
                else if (isExternal) { fill = '#ffffff'; stroke = '#ffa39e'; }
                
                const keyShape = group.addShape('rect', {
                    attrs: { x: -width / 2, y: -height / 2, width, height, fill, stroke, lineWidth, radius: 2, cursor: 'pointer' }, name: 'table-container',
                });

                group.addShape('text', {
                    attrs: { x: -10, y: 0, textAlign: 'center', textBaseline: 'middle', text: label, fill: textColor, fontSize, cursor: 'pointer' }, name: 'table-label',
                });

                if (relationLabel && depth > 2) {
                    group.addShape('text', {
                        attrs: { x: 0, y: -height / 2 - 4, textAlign: 'center', textBaseline: 'bottom', text: relationLabel, fill: '#1890ff', fontSize: 7,  fontWeight: 'bold' },
                        name: 'relation-label',
                    });
                }

                if (!isRoot && level != 1) {
                    group.addShape('circle', {
                        attrs: { x: (width / 2) - 12, y: 0, r: 7, fill: isSelected ? (selectIconColor || '#3c970eff') : '#2c3e50', cursor: 'pointer' }, name: 'select-icon-bg',
                    });

                    const isInPipeline = cfg.isInPipeline || false;
                    let textContent = { fill: '#8c8c8c', text: 'Plan', fontSize: 7 } ;
                    if(isInPipeline)
                        textContent = { fill: '#059669', text: '✔', fontSize: 9 };
                    
                    group.addShape('text', {
                        attrs: { x: (width / 2) - 30, y: 0, textAlign: 'center', textBaseline: 'middle', ...textContent, fontWeight: 'bold', cursor: 'pointer' }, name: 'pipeline-tick-icon',
                    });

                    let text = isSelected ? (orderNumber || '-') : '+', fill = '#fff';
                    group.addShape('text', {
                        attrs: { x: (width / 2) - 12, y: 0, textAlign: 'center', textBaseline: 'middle', text, fill, fontSize: 9, fontWeight: 'bold', cursor: 'pointer' },
                        name: 'select-icon-text',
                    });
                }
                return keyShape;
            }
        });
    }

    static calculateTextWidth(text, font = '8px Arial') {
        const context = document.createElement('canvas').getContext('2d');
        context.font = font;
        return context.measureText(text || '').width;
    }

    nodeMap = new Map()
    anchorNode = null;
    listToTree(data, moduleName, relations = []) {
        const prefix = `${moduleName.toLowerCase()}_`;

        this.nodeMap = new Map();
        const sorted = [...data].sort((a, b) => a[0] - b[0]);
        
        const getRelationLabel = (sCol, tCol) => {
            if (!sCol || !tCol) return {};
            const cardinality = (tCol === 'id') ? 'N:1' : '1:1';
            return { relationLabel: `(${cardinality}) ${sCol} ➔ ${tCol}` };
        };

        sorted.forEach(row => {
            const [level, parentTable, childTable, fullPath, sCol, tCol, , allFields, anchor] = row;
            if(parentTable === '(root)') return;          

            if(level == 1) this.pipelineTableFields.set(parentTable, allFields.split(','));
            else this.pipelineTableFields.set(childTable, allFields.split(','));
            
            //if (level !== 2 || !parentTable?.startsWith(prefix)) return;
            
            const key = level == 0 ? fullPath : `${fullPath}`.split(' -> ').slice(0, -1).join(' -> ');
            const item = { id: fullPath, label: childTable, children: [], collapsed: true, allFields, ...getRelationLabel(sCol, tCol), moduleName, anchor, clickable: true };
            let /** @type { { children: Array } } */ parent = this.nodeMap.get(key);

            if (parent === undefined) {
                this.nodeMap.set(key, { id: key, label: parentTable, children: [], collapsed: !(level === 0), allFields, ...getRelationLabel(sCol, tCol), moduleName, anchor, clickable: true });
                parent = this.nodeMap.get(key);
                if(parentTable === anchor && level === 1) this.anchorNode = parent;
            }

            if(level > 0) {
                this.nodeMap.set(fullPath, item);
                const child = this.nodeMap.get(fullPath);
                parent.children.push(child);
            }
        });
        
        return Array.from(this.nodeMap.values()).filter(node => node.id.split(' -> ').length === 1);
    }

    async loadMonacoEditorDependencies(){
        
        if (window.monaco) return;
        
        await Assets.import({ path: 'https://cdn.jsdelivr.net/npm/showdown/dist/showdown.min.js' });
        await Assets.import({ path: 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.46.0/min/vs/loader.min.js' });

        require.config({ paths: { 'vs': 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.46.0/min/vs' } });
        require(['vs/editor/editor.main'], (monaco) => {
            monaco.languages.registerCompletionItemProvider('python', {
                provideCompletionItems: () => ({ suggestions: CodeEditorUtil.getPythonSuggestions() }),
            });
            window.monaco
        });

    }

    diagramContainer = () => BIService.getDBDiagramContainer();

    async selectConnectionName(connectionName){
        this.toggleView('diagram');
        BIController.fromContext().addLoadingOnContainer(this.diagramContainer(), 'Loading database diagram');
        const result = await BIService.getModulesWhenOdoo(connectionName);
        
        //this.obj.updateGraphData(result?.modules);
        BIController2.fromContext().renderExplorerTables(result?.tables);
        BIService.selectedConnection = connectionName, this.selectedDatabase = result?.db_name;
        this.selectedConnectionHost = result?.db_host, this.selectedConnectionEngine = result?.db_engine;
        BIController.fromContext().removeLoadingFromContainer(this.diagramContainer());
    }

    async loadCodeEditor(){
        if(!!this.editor) return;
        if(!this.obj.$parent.runningOnOdoo)
            this.loadMonacoEditorDependencies();

        this.editor = monaco.editor.create(document.getElementById('sqlEditor'), {
            value: this.query, language: 'sql', theme: 'vs-light', automaticLayout: true, fontSize: 14, minimap: { enabled: false }, scrollBeyondLastLine: false,
        });
        this.editor.setValue(`-- Write your query here. \n-- Or Add/Select tables in the diagram to the query.`);
    }

    async runSQLQuery(){
        const container = this.obj.container.querySelector('.sqlDataExploratio');
        BIController.fromContext().addLoadingOnContainer(container, 'Fetching/Processing data');

        const result = await BIService.runSQLQuery(this.editor.getValue());
        const fields = result.fields || [], rows = result.result;
        
        if(!this.datagridInstance){
            const { template: gridUI, component: gridComponent } = await Components.new(Grid, { fields, data: rows });
            this.datagridInstance = gridComponent;
            this.obj.container.querySelector('.queryResultPLaceholder').innerHTML = gridUI;
            this.datagridInstance.onLoad(() => {
                this.datagridInstance.loadGrid();
                BIController.fromContext().removeLoadingFromContainer(container);
            });
        }else{
            this.datagridInstance.setGridData(fields, rows).loadGrid();
            BIController.fromContext().removeLoadingFromContainer(container);
        }
    }

    setGraphOnClickEvt(graph){

        graph.on('node:click', async (e) => {
            const { item, target } = e;
            const model = item.getModel(), shapeName = target.get('name');

            if (shapeName === 'pipeline-tick-icon') {
                this.togglePipelineTable(model.label, item, model.moduleName);
                this.plannerMode = 'regular';
                this.clearReview('regular');
                return graph.updateItem(item, { isInPipeline: this.pipelineTables.has(model.label) });
            }

			if (shapeName === 'select-icon-bg' || shapeName === 'select-icon-text') {
				this.handleTableSelect(model.label);
				
				const order = this.selectedTablesMap.get(model.label), isUnrelated = this.isTableUnrelated(model.label);

				graph.updateItem(item, { isSelected: !!order, orderNumber: order || '', selectIconColor: isUnrelated ? '#9E9E9E' : '#4CAF50' });
				return this.syncSqlEditor();
			}

            if (model.children && model.children.length > 0) {
                graph.updateItem(item, { collapsed: !model.collapsed });
                return graph.layout();
            }

            if (model.clickable) {
                const moduleName = model.label, anchor = model.anchor; 
                graph.updateItem(item, { label: `${moduleName} (Loading...)` });

                try {
                    await this.renderGraph(moduleName.toLowerCase(), anchor);
                } catch (err) {
                    console.error('Failed to load module tables:', err);
                    graph.updateItem(item, { label: moduleName });
                }
            }
        });

    }

    async renderGraph(tableName, anchor){
        const result = await BIService.getTablesWhenOdoo(anchor || tableName);
        this.compileRelations(result.relations);
        const children = this.listToTree(result.tables, tableName, result.relations);
        this.obj.graph.updateItem(item, { label: tableName, children, collapsed: false });
        this.obj.graph.layout();
    }

    rvewPlanSlctdFieldByTbl = new Map();
    
    togglePipelineTable(tablePath, item, moduleName, selectedFlds, review) {
        
        let [module, tableName] = this.parseFieldMap(tablePath, true, true); 
        if(module) moduleName = module;
        if (this.pipelineTables.has(tableName) && (review !== true)){
            this.removeFromPipeline(tableName);
        } else {
            if(selectedFlds){
                Object.entries(selectedFlds).forEach(([fld, type]) => {
                    fld = type?.toLowerCase() === 'pk' ? `${fld} (${type})` : fld;
                    this.selectedFieldsSet.add(`${moduleName ? moduleName+'.' : ''}${tableName}.${fld}`);
                });
                this.rvewPlanSlctdFieldByTbl.set(tableName,selectedFlds);
            } 
            this.pipelineTables.set(tableName, { name: tableName, isExpanded: false, item });
            this.obj.pipelineTablesList = Array.from(this.pipelineTables.values());
            this.obj.totalTablesAdded = this.pipelineTables.size;
            this.selectedTablesMap.set(tableName, this.selectedTablesMap.size);

            const newPlanedTable = this.obj.parseEvents(`
				<div each="item" class="item-container item-container-${tableName}">
					<div class="item-main">
						<div style="display: flex; align-items: center; gap: 8px;">
							<span class="expand-table-columns expand-table-columns-${tableName}" onclick="controller.toggleTableFields('${tableName}','${moduleName}')">▶</span>
							<span class="item-name">${tableName}</span>
						</div>
						<button class="remove-btn" onclick="controller.removeFromPipeline('${tableName}')">×</button>
					</div>
					<div class="item-fields-expanded" style="margin-top: 8px; padding-left: 18px; border-left: 2px solid #e5e7eb;">
						<div class="list-of-tables-in-plan list-of-tables-in-plan-${tableName}"></div>
					</div>
				</div>
            `);
            this.obj.container.querySelector('.planner-item').insertAdjacentHTML('beforeend', newPlanedTable);
        }
    }

    /** @type { Set<string> } */ selectedFieldsSet = new Set();

    selectColumn(fieldPath, moduleName) {
        let shouldKeepTable, tableName = fieldPath.split('.')[0], container = this.obj.container;
        const fieldPrefix = `${(moduleName && !(fieldPath.split('.').length > 1)) ? moduleName+'.' : ''}`;
        let pkPresent = [...this.selectedFieldsSet].map(this.parseFieldMap).find(itm => itm.startsWith(tableName) && itm.toLowerCase().includes('(pk)'));
        const newFieldPath = `${fieldPrefix}`+fieldPath;

        if (this.selectedFieldsSet.has(newFieldPath)) {
            this.selectedFieldsSet.delete(newFieldPath);
        } else {
            if(!container.querySelector(`.list-of-tables-in-plan-${tableName}`).classList.contains('addedkey')){
                const tablePk = this.pipelineTableFields.get(tableName).find(itm => itm.toLowerCase().includes('(pk)'));
                this.selectedFieldsSet.add(`${fieldPrefix}${tableName}.${tablePk}`);
                container.querySelector(`.list-of-tables-in-plan-${tableName}`).classList.add('addedkey');
            }
            
            this.selectedFieldsSet.add(newFieldPath);
            container.querySelectorAll(`.plan-pipeline-pk-${tableName}`).forEach(itm => itm.checked = true);
        }
        
        if (!this.selectedTablesMap.has(tableName)) 
            this.handleTableSelect(tableName);

        shouldKeepTable = [...this.selectedFieldsSet].map(this.parseFieldMap).some(itm => itm.startsWith(fieldPath.split('.')[0]) && itm != pkPresent);
        if(!shouldKeepTable){
            this.selectedTablesMap.delete(tableName);
            if(pkPresent){
                container.querySelectorAll(`.plan-pipeline-pk-${tableName}`).forEach(itm => itm.checked = false);
                this.selectedFieldsSet.delete(pkPresent);
                container.querySelector(`.list-of-tables-in-plan-${tableName}`).classList.remove('addedkey');
            }
        }
        this.editorSelectedCode = null;
        this.syncSqlEditor();
    }

    removeFromPipeline(tableName) { 
        const entry = this.pipelineTables.get(tableName);
        if (!entry) return;

        if (this.selectedTablesMap.has(tableName)) {
            this.selectedTablesMap.delete(tableName);
            
            for (const fieldPath of this.selectedFieldsSet) {
                if (fieldPath.startsWith(`${tableName}.`)) this.selectedFieldsSet.delete(fieldPath);
            }

            const remaining = Array.from(this.selectedTablesMap.keys());
            this.selectedTablesMap.clear();
            remaining.forEach((name, i) => this.selectedTablesMap.set(name, i + 1));
        }

        this.pipelineTables.delete(tableName);
        if (entry.item && typeof entry.item.getStates === 'function' && !entry.item.destroyed) {
            this.obj.graph.updateItem(entry.item, { isInPipeline: false, isSelected: false, orderNumber: '' });
        }
        
        const container = this.obj.container.querySelector(`.item-container-${tableName}`);
        if (container) container.remove();

        this.obj.totalTablesAdded = this.pipelineTables.size;
        this.syncSqlEditor();
    }

    loadExistingPipeline(val){
        const planeNameInput = this.obj.container.querySelector('.plan-inputs1 input');
        const saveBtn = this.obj.container.querySelector('.save-plan-btn');
        this.selectedExistingPpline = val;
        if(val !== '') {
            planeNameInput.disabled = true, saveBtn.disabled = false;
        } else {
            planeNameInput.disabled = false;
            if([undefined, null,''].includes(this.pipelineName)) saveBtn.disabled = true;
            else saveBtn.disabled = false;
        }
    }

    setPipelineName(val) { 
        const inputs = this.obj.container.querySelectorAll('.plan-inputs2 select, .save-plan-btn');
        if(val !== '') inputs.forEach(ipt => ipt.disabled = false);
        else inputs.forEach((ipt, idx) => ipt.disabled = idx == 0 ? false : true);
        
        this.pipelineName = val; 
    }

    async toggleTableFields(tableName, moduleName) {
        const tableEntry = this.pipelineTables.get(tableName);
        if (!tableEntry) return;

        tableEntry.isExpanded = !tableEntry.isExpanded;

        const fieldsContainer = this.obj.container.querySelector(`.list-of-tables-in-plan-${tableName}`);
        if (tableEntry.isExpanded) {
            this.obj.container.querySelector(`.expand-table-columns-${tableName}`).textContent = '▼';
            
            if(fieldsContainer.style.display == 'none'){
                fieldsContainer.style.display = '';
            } else {
                let fields = this.pipelineTableFields.get(tableName);
                if(!fields){
                    const result = await BIService.getTablesWhenOdoo(moduleName.toLowerCase(), true);                    
                    this.compileRelations(result.relations);                    
                    this.listToTree(result.tables, moduleName, result.relations);
                    fields = this.pipelineTableFields.get(tableName);
                }
                const selectedFields = Object.keys(this.rvewPlanSlctdFieldByTbl.get(tableName) || {});

                fieldsContainer.innerHTML = fields.map(fld => {
                    fld = fld.trim();
                    const isPk = fld.toLowerCase().includes('(pk)'), value = fld.split(' ')[0]?.trim();
                    const fieldPath = `${tableName}.${fld}`;
                    
                    const hasQuality = this.columnQualityRules && this.columnQualityRules.has(fieldPath);

                    return this.obj.parseEvents(`
                        <div class="field-item-container" style="position: relative; border-bottom: 1px solid #eee;">
                            <div style="display: flex; align-items: center; justify-content: space-between; padding: 4px 0;">
                                <div style="display: flex; align-items: center;">
                                    <input type="checkbox"  value="${value}"  ${selectedFields.includes(value) ? 'checked' : ''}
                                        ${isPk ? `class="plan-pipeline-pk-${tableName}" disabled checked` : ''}  onclick="controller.selectColumn('${fieldPath}','${moduleName}')"> 
                                    <span style="margin-left: 4px; font-size: 11px;">${fld}</span>
                                </div>
                                
                                <span class="dq-gear-icon" 
                                    style="cursor: pointer; font-size: 10px; margin-right: 5px; opacity: ${hasQuality ? '1' : '0.4'}; color: ${hasQuality ? '#22c55e' : '#6b7280'}"
                                    onclick="controller('DataQualityController').toggleDQMenu(event, '${fieldPath}')">
                                    ⚙️
                                </span>
                            </div>

                            <div id="dq-menu-${fieldPath.replace(/[^a-zA-Z0-9]/g, '_')}" class="dq-dropdown" style="display: none; background: #fff; border: 1px solid #ccc; position: absolute; right: 0; z-index: 100; box-shadow: 0 2px 5px rgba(0,0,0,0.2); width: 120px; border-radius: 4px;">
                                <div style="padding: 5px; font-size: 10px; font-weight: bold; border-bottom: 1px solid #eee; background: #f9f9f9;">Transform</div>
                                ${['TRIM', 'UPPER', 'LOWER', 'COALESCE'].map(rule => `
                                    <div class="dq-option" style="padding: 4px 8px; font-size: 10px; cursor: pointer;" 
                                        onclick="controller('DataQualityController').applyRule('${fieldPath}', '${rule}', '${tableName}', '${moduleName}')">
                                        ${rule}
                                    </div>
                                `).join('')}
                                <div class="dq-option" style="padding: 4px 8px; font-size: 10px; cursor: pointer; color: red; border-top: 1px solid #eee;" 
                                    onclick="controller('DataQualityController').applyRule('${fieldPath}', null, '${tableName}', '${moduleName}')">
                                    Clear
                                </div>
                            </div>
                        </div>
                    `);
                }).join('');
            }
        } else {
            this.obj.container.querySelector(`.expand-table-columns-${tableName}`).textContent = '▶';
            fieldsContainer.style.display = 'none';
        }
    }

    clearReview(plannerMode, force){
        if((this.plannerMode !== plannerMode && this.plannerMode !== 'review') || force === true){
            this.rvewPlanSlctdFieldByTbl.clear(), this.obj.graph.clear();
            this.pipelineTables.clear(), this.selectedTablesMap.clear(), this.selectedFieldsSet.clear();
            this.editorSelectedCode = null, this.selectedPlanId = null, this.obj.saveBtnLabel = 'Save', this.pipelineName = null;;
            this.obj.container.querySelector('.plan-inputs1 input').value = '', this.obj.container.querySelector(`.DatabaseDiagram-app-controls select`).value = '';
            this.obj.container.querySelector('.planner-item').innerHTML = '', this.obj.container.querySelector('.save-plan-btn').disabled = true;
            this.editor.setValue(`-- Write your query here. \n-- Or Add/Select tables in the diagram to the query.`);
        }
    }

    listOfPlans = new Map();
    editorSelectedCode = null;
    async selectPlanView(name, planName){
        const tab = name === 'review' ? 'create' : name;
        this.obj.container.querySelectorAll(`.planner-title span`).forEach(itm => 
            itm.classList.contains(tab) ? itm.classList.remove('inactive') : itm.classList.add('inactive')
        );
        this.obj.container.querySelectorAll(`.plan-wrapper`).forEach(itm => 
            itm.classList.contains(tab) ? itm.style.display = 'flex' : itm.style.display = 'none'
        );
        
        if(name === 'review'){
            this.clearReview(name, true), this.plannerMode = name;
            const reviewData = JSON.parse(this.listOfPlans.get(planName).plan);
            const dataSource = reviewData.content.Home.data[2].data;
            
            this.obj.container.querySelector('.plan-inputs1 input').value = reviewData.pipeline_lbl, this.obj.container.querySelector('.save-plan-btn').disabled = false;
            this.obj.container.querySelector(`.DatabaseDiagram-app-controls select`).value = dataSource.connectionName, this.pipelineName = reviewData.pipeline_lbl;
            
            this.selectConnectionName(dataSource.connectionName), this.obj.saveBtnLabel = 'Update';
            
            Object.entries(dataSource.tables).map(this.parseFieldMap).forEach(([tblField, selFlds]) => this.togglePipelineTable(tblField, {}, undefined, selFlds, true));
            this.editorSelectedCode = reviewData.goldQuery, this.selectedPlanId = this.listOfPlans.get(planName).id;
            this.syncSqlCount = 0;
        }

        if(name === 'view'){
            let plans = await PipelinePlanService.getPipelinePlans(), planNames = [];
            for(const plan of (plans.result || [])){
                this.listOfPlans.set(plan.pipeline_lbl, { plan: plan.plan_setting, id: plan.id });
                planNames.push({ name: plan.pipeline_lbl, processed: plan.processed });
            }
            this.obj.pipelinePlans = [...planNames];
        }
    }

    minizePlanner(){
        const container = this.obj.container.querySelector('.pipeline-planner-panel');
        const icon = this.obj.container.querySelector('.planner-minimize-icon');
        if(container.style.height == 'auto'){
            container.style.height = '42px', icon.innerHTML = '&#9634;';
        } else {
            container.style.height = 'auto', icon.innerHTML = '&minus;';
        }
    }

    async savePipelinePlan(){
        let settings = new PipelinePlanPayload(), pkCount = 0, saveBtn = this.obj.container.querySelector('.save-plan-btn');
        saveBtn.disabled = true;
        this.selectedFieldsSet.forEach(fld => {
            let [moduleName, itm] = this.parseFieldMap(fld, true);
            let [table, field] = itm.split('.');

            table = `${moduleName ? moduleName+'.' : ''}${table}`;
            
            const isPk = field.toLowerCase().trim().endsWith('(pk)');
            if(isPk){
                pkCount++, field = field.split(' ')[0] //In case of PK, extract only the field name
                if(pkCount == 1) settings.primaryKeys['pkName'] = field;
                else settings.primaryKeys[`primaryKey${pkCount}`] = field;
            }
            if(!settings.tables[table]) settings.tables[table] = {}
            settings.tables[table][field] = isPk ? 'PK' : null;
        });
        settings.goldTableQuery = this.editor.getValue(), settings.planPipelineLabel = this.pipelineName;
        settings.sourceDatabase = this.selectedDatabase;
        settings.sourceDBConnection = this.selectedConnection;
        settings.sourceDBHost = this.selectedConnectionHost;
        settings.sourceDBEngine = this.selectedConnectionEngine;
        settings.planNamespace = await BIService.getNamespace();
        await (new PipelinePlanService(settings)).save(this.obj.saveBtnLabel.value === 'Update', this.selectedPlanId);
        saveBtn.disabled = false;
    }
}