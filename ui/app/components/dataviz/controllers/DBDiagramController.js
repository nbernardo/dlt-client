import { BaseController } from "../../../../@still/component/super/service/BaseController.js";
import { DatabaseDiagram } from "../diagram/DatabaseDiagram.js";

export class DBDiagramController extends BaseController {
    
    /** @type { DatabaseDiagram } */
    obj;

    /** @type { Map<string, number> } */
    selectedTablesMap = new Map(); // table_name -> orderNumber

    relationRegistry = new Map(); 

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
                this.relationRegistry.set(relKey, {
                    sourceTable: sTable,
                    sourceColumn: sCol,
                    targetTable: tTable,
                    targetColumn: tCol
                });
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
        const editorEl = this.obj.container.querySelector('#sqlEditor');
        const btnDiagram = this.obj.container.querySelector('#btnDiagram');
        const btnSQL = this.obj.container.querySelector('#btnSQL');

        if (view === 'diagram') {
            diagramEl.style.display = 'block';
            editorEl.style.display = 'none';
            btnDiagram.classList.add('active');
            btnSQL.classList.remove('active');
            if (this.obj.graph) this.obj.graph.changeSize(diagramEl.scrollWidth, diagramEl.scrollHeight);
        } else {
            diagramEl.style.display = 'none';
            editorEl.style.display = 'block';
            btnDiagram.classList.remove('active');
            btnSQL.classList.add('active');
            this.syncSqlEditor();
        }
    }

    syncSqlEditor() {
        const textarea = this.obj.container.querySelector('#sqlTextarea');
        if (!textarea) return;

        const selectedEntries = Array.from(this.selectedTablesMap.entries());
        if (selectedEntries.length === 0) {
            textarea.value = `-- e2e-Data Query Builder\n-- Select tables in the diagram to auto-generate JOINs.`;
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
        textarea.value = [
            `-- Auto-generated Business Query`,
            `SELECT *`,
            `FROM ${fromClause}`,
            joins.length > 0 ? `    ${joins.join('\n    ')}` : ''
        ].filter(v => v.trim()).join('\n') + ';';
    }

    static initCustomDBNode() {
        G6.registerNode('db-table', {
            draw(cfg, group) {
                const { label = '', isRoot, isExternal, isSelected, orderNumber, selectIconColor, level } = cfg;
                
                const fontSize = 8, height = 20;
                const width = DBDiagramController.calculateTextWidth(label, `${fontSize}px Arial`) + 40;

                let fill = cfg.level === 1 ? '#e6f7ff' : '#f0f5ff';
                let stroke = isSelected ? '#1890ff' : (cfg.level === 1 ? '#91d5ff' : '#adc6ff');
                let textColor = isRoot ? '#ffffff' : '#000000';
                let lineWidth = isSelected ? 2 : 1;

                if (isRoot) { fill = '#1d39c4'; stroke = '#002329'; } 
                else if (isExternal) { fill = '#ffffff'; stroke = '#ffa39e'; }

                const keyShape = group.addShape('rect', {
                    attrs: { x: -width / 2, y: -height / 2, width, height, fill, stroke, lineWidth, radius: 2, cursor: 'pointer' },
                    name: 'table-container',
                });

                group.addShape('text', {
                    attrs: { x: -10, y: 0, textAlign: 'center', textBaseline: 'middle', text: label, fill: textColor, fontSize, cursor: 'pointer' },
                    name: 'table-label',
                });

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

    listToTree(data, moduleName) {
        const prefix = `${moduleName.toLowerCase()}_`;
        const moduleRootPath = moduleName.toUpperCase();
        const nodeMap = new Map();
        const sorted = [...data].sort((a, b) => a[0] - b[0]);

        sorted.forEach(row => {
            const [level, parentTable] = row;
            if (level !== 2 || !parentTable?.startsWith(prefix)) return;
            const key = `${moduleRootPath} -> ${parentTable}`;
            if (!nodeMap.has(key)) {
                nodeMap.set(key, { id: key, label: parentTable, isExternal: false, children: [], collapsed: true });
            }
        });

        sorted.forEach(row => {
            const [level, parentTable, childTable, fullPath] = row;
            if (level < 3 || !fullPath || !childTable) return;
            const segments = fullPath.split(' -> ');
            const parentPathKey = segments.slice(0, -1).join(' -> ');

            if (!nodeMap.has(fullPath)) {
                nodeMap.set(fullPath, { id: fullPath, label: childTable, isExternal: !childTable.startsWith(prefix), children: [], collapsed: true });
            }
            const parentNode = nodeMap.get(parentPathKey);
            if (parentNode && !parentNode.children.find(c => c.id === fullPath)) 
                parentNode.children.push(nodeMap.get(fullPath));
        });

        return Array.from(nodeMap.values()).filter(node => node.id.split(' -> ').length === 2);
    }
}