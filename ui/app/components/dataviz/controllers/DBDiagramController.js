import { BaseController } from "../../../../@still/component/super/service/BaseController.js";
import { Assets } from "../../../../@still/util/componentUtil.js";
import { Grid } from "../bi/grid/Grid.js";
import { DatabaseDiagram } from "../diagram/DatabaseDiagram.js";
import { BIService } from "../services/BIService.js";
import { BIController } from "./BIController.js";

export class DBDiagramController extends BaseController {
    
    /** @type { DatabaseDiagram } */
    obj;

    editor;

    selectedConnection;

    /** @type { Map<string, number> } */
    selectedTablesMap = new Map(); // table_name -> orderNumber

    relationRegistry = new Map(); 

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
        const selected = Array.from(this.selectedTablesMap.keys());
        const relations = Array.from(this.relationRegistry.values());

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

    syncSqlEditor() {

        const selectedEntries = Array.from(this.selectedTablesMap.entries());
        if (selectedEntries.length === 0) {
            this.editor.setValue(`-- Write your query here. \n-- Or Add/Select tables in the diagram to the query.`);
            return;
        }

        const [baseTable] = selectedEntries[0];
        const processed = new Set([baseTable]);
        const joins = [];
        const crossJoins = [];
        const relations = Array.from(this.relationRegistry.values());

        selectedEntries.slice(1).forEach(([currentTable]) => {
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
            } else {
                //crossJoins.push(currentTable);
                //processed.add(currentTable);
            }
        });

        const fromClause = [baseTable, ...crossJoins].join(', ');
        this.editor.setValue(
            [
                `-- Auto-generated Business Query`,
                `SELECT * FROM ${fromClause}`,
                joins.length > 0 ? `    ${joins.join('\n    ')}` : '',
                'LIMIT 100'
            ].filter(v => v.trim()).join('\n') + ';'
        );        
    }

    static initCustomDBNode() {
        G6.registerNode('db-table', {
            draw(cfg, group) {
                const { label = '', isRoot, isExternal, isSelected, orderNumber, selectIconColor, level, relationLabel, depth } = cfg;
                const fontSize = 8, height = 20;
                const width = DBDiagramController.calculateTextWidth(label, `${fontSize}px Arial`) + 40;

                let fill = cfg.level === 1 ? '#e6f7ff' : '#f0f5ff';
                let stroke = isSelected ? '#1890ff' : (cfg.level === 1 ? '#91d5ff' : '#adc6ff');
                let textColor = isRoot ? '#ffffff' : '#000000';
                let lineWidth = isSelected ? 2 : 1;

                if (isRoot) { fill = '#2c3e50'; stroke = '#002329'; } 
                else if (isExternal) { fill = '#ffffff'; stroke = '#ffa39e'; }

                const keyShape = group.addShape('rect', {
                    attrs: { x: -width / 2, y: -height / 2, width, height, fill, stroke, lineWidth, radius: 2, cursor: 'pointer' },
                    name: 'table-container',
                });

                group.addShape('text', {
                    attrs: { x: -10, y: 0, textAlign: 'center', textBaseline: 'middle', text: label, fill: textColor, fontSize, cursor: 'pointer' },
                    name: 'table-label',
                });

                if (relationLabel && depth > 2) {
                    group.addShape('text', {
                        attrs: { x: 0, y: -height / 2 - 4, textAlign: 'center', textBaseline: 'bottom', text: relationLabel, fill: '#1890ff', fontSize: 7,  fontWeight: 'bold' },
                        name: 'relation-label',
                    });
                }

                if (!isRoot && level != 1) {
                    group.addShape('circle', {
                        attrs: { 
                            x: (width / 2) - 12, y: 0, r: 7, fill: isSelected ? (selectIconColor || '#3c970eff') : '#2c3e50', cursor: 'pointer' 
                        },
                        name: 'select-icon-bg',
                    });

                    group.addShape('text', {
                        attrs: {
                            x: (width / 2) - 12, y: 0, textAlign: 'center', textBaseline: 'middle', text: isSelected ? (orderNumber || '-') : '+', 
                            fill: '#fff', fontSize: 9, fontWeight: 'bold', cursor: 'pointer'
                        },
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

    listToTree(data, moduleName, relations = []) {
        const prefix = `${moduleName.toLowerCase()}_`;
        const moduleRootPath = moduleName.toUpperCase();
        const nodeMap = new Map();
        const sorted = [...data].sort((a, b) => a[0] - b[0]);

        const getRelationLabel = (sCol, tCol) => {
            if (!sCol || !tCol) return {};
            const cardinality = (tCol === 'id') ? 'N:1' : '1:1';
            return { relationLabel: `(${cardinality}) ${sCol} ➔ ${tCol}` };
        };

        sorted.forEach(row => {
            const [level, parentTable, childTable, fullPath, sCol, tCol] = row;
            
            if (level !== 2 || !parentTable?.startsWith(prefix)) return;

            const key = `${moduleRootPath} -> ${parentTable}`;

            if (!nodeMap.has(key)) {
                nodeMap.set(key, { id: key, label: parentTable, isExternal: false, children: [], collapsed: true, ...getRelationLabel(sCol, tCol)});
            }
        });

        sorted.forEach(row => {
            const [level, parentTable, childTable, fullPath, sCol, tCol] = row;
            if (level < 3 || !fullPath || !childTable) return;

            const segments = fullPath.split(' -> ');
            const parentPathKey = segments.slice(0, -1).join(' -> ');

            if (!nodeMap.has(fullPath)) {
                nodeMap.set(fullPath, { id: fullPath, label: childTable, isExternal: !childTable.startsWith(prefix), children: [], collapsed: true, ...getRelationLabel(sCol, tCol) });
            }
            
            const parentNode = nodeMap.get(parentPathKey);
            if (parentNode && !parentNode.children.find(c => c.id === fullPath)) {
                parentNode.children.push(nodeMap.get(fullPath));
            }
        });

        return Array.from(nodeMap.values()).filter(node => node.id.split(' -> ').length === 2);
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

    async selectConnectionName(connectionName){
        const container = this.obj.container.querySelector('#mountNode');
        BIController.fromContext().addLoadingOnContainer(container, 'Loading database diagram');
        const result = await BIService.getModulesWhenOdoo(connectionName);
        this.obj.updateGraphData(result);
        this.selectedConnection = connectionName;
        BIController.fromContext().removeLoadingFromContainer(container);
    }

    async loadCodeEditor(){
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

        const result = await BIService.runSQLQuery(this.editor.getValue(), this.selectedConnection);
        const fields = result.fields || [];
        const rows = result.result;
        
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

			if (shapeName === 'select-icon-bg' || shapeName === 'select-icon-text') {
				this.handleTableSelect(model.label);
				
				const order = this.selectedTablesMap.get(model.label);
				const isUnrelated = this.isTableUnrelated(model.label);

				graph.updateItem(item, { isSelected: !!order, orderNumber: order || '', selectIconColor: isUnrelated ? '#9E9E9E' : '#4CAF50' });
				return this.syncSqlEditor();
			}

            if (model.children && model.children.length > 0) {
                graph.updateItem(item, { collapsed: !model.collapsed });
                return graph.layout(); 
            }

            if (model.id.startsWith('folder:')) {
                const moduleName = model.label; 
                graph.updateItem(item, { label: `${moduleName} (Loading...)` });

                try {
                    const result = await BIService.getTablesWhenOdoo(moduleName.toLowerCase(), this.selectedConnection);                    
                    this.compileRelations(result.relations);                    
                    const children = this.listToTree(result.tables, moduleName, result.relations);
                    graph.updateItem(item, { label: moduleName, children: children, collapsed: false });
                    graph.layout();
                } catch (err) {
                    console.error('Failed to load module tables:', err);
                    graph.updateItem(item, { label: moduleName });
                }
            }
        });

    }    
}