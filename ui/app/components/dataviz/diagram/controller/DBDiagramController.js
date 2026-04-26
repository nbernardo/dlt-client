import { BaseController } from "../../../../../@still/component/super/service/BaseController.js";
import { DatabaseDiagram } from "../DatabaseDiagram.js";

export class DBDiagramController extends BaseController{
    
    /** @type { DatabaseDiagram } */
    obj;

    /** @type { Set<string> } */
    selectedTables = new Set();

    relationRegistry = new Map(); 

    /** @param {Array} rows */
    compileRelations(rows) {
        rows.forEach(row => {
            // [1] sCol, [2] sTable, [3] tCol, [4] tTable
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

            // Still.js component reflow check: Ensure G6 resizes to visible space
            if (this.obj.graph) {
                this.obj.graph.changeSize(diagramEl.scrollWidth, diagramEl.scrollHeight);
            }
        } else {
            diagramEl.style.display = 'none';
            editorEl.style.display = 'block';
            btnDiagram.classList.remove('active');
            btnSQL.classList.add('active');
            
            // Focus and populate query if needed
            this.syncSqlEditor();
        }
    }

    syncSqlEditor() {
        const textarea = this.obj.container.querySelector('#sqlTextarea');
        if (!textarea || textarea.value) return; // Only set if empty or you have new query

        textarea.value = `-- Odoo Business Logic Query\nWITH RECURSIVE TableHierarchy AS (...`;
    }

    static initCustomDBNode() {
        G6.registerNode('db-table', {
            draw(cfg, group) {
                const { label = '', isRoot, isExternal, isSelected } = cfg;
                const fontSize = 8, height = 20;
                const width = DBDiagramController.calculateTextWidth(label, `${fontSize}px Arial`) + 30;

                let fill = cfg.level === 1 ? '#e6f7ff' : '#f0f5ff';
                let stroke = cfg.level === 1 ? '#91d5ff' : '#adc6ff';
                let textColor = isRoot ? '#ffffff' : '#000000';
                let lineDash = null;

                if (isRoot) {  fill = '#1d39c4', stroke = '#002329';  } 
                else if (isExternal) { fill = '#ffffff', stroke = '#ffa39e', lineDash = [3, 2]; }

                if (isSelected) {
                    stroke = '#1890ff';
                    var lineWidth = 2;
                } else 
                    var lineWidth = 1;

                const keyShape = group.addShape('rect', {
                    attrs: { x: -width / 2, y: -height / 2, width, height, fill, stroke, lineWidth, radius: 2, lineDash },
                    name: 'table-container',
                });

                group.addShape('text', {
                    attrs: { 
                        x: -10, y: 0, textAlign: 'center', textBaseline: 'middle',  text: label, fill: textColor, fontSize, 
                        fontWeight: isRoot ? 'bold' : 'normal' 
                    },
                    name: 'table-label',
                });

                if (!isRoot) {
                    group.addShape('circle', {
                        attrs: { x: (width / 2) - 12, y: 0, r: 6, fill: isSelected ? '#ff4d4f' : '#52c41a', cursor: 'pointer' },
                        name: 'select-icon-bg',
                    });

                    group.addShape('text', {
                        attrs: {
                            x: (width / 2) - 12, y: 0, textAlign: 'center', textBaseline: 'middle', text: isSelected ? '-' : '+', 
                            fill: '#fff', fontSize: 10, fontWeight: 'bold', cursor: 'pointer'
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
            const [level, parentTable, childTable, fullPath] = row;
            if (level !== 2) return;
            if (!parentTable?.startsWith(prefix)) return;

            const directPathKey = `${moduleRootPath} -> ${parentTable}`;
            if (!nodeMap.has(directPathKey)) {
                const label = parentTable, isExternal = false, children = [], collapsed =  true;
                nodeMap.set(directPathKey, { id: directPathKey, label, isExternal, children, collapsed });
            }
        });


        sorted.forEach(row => {
            const [level, parentTable, childTable, fullPath] = row;
            if (level < 3 || !fullPath || !childTable) return;

            const segments = fullPath.split(' -> ');
            const nodePathKey = fullPath;
            const parentPathKey = segments.slice(0, segments.length - 1).join(' -> ');

            if (!nodeMap.has(nodePathKey)) {
                const label = childTable, isExternal = !childTable.startsWith(prefix), children = [], collapsed = true;
                nodeMap.set(nodePathKey, { id: nodePathKey, label, isExternal, children, collapsed });
            }

            const parentNode = nodeMap.get(parentPathKey);
            if (parentNode && !parentNode.children.find(c => c.id === nodePathKey)) 
                parentNode.children.push(nodeMap.get(nodePathKey));
        });

        return Array.from(nodeMap.values()).filter(node => {
            const segments = node.id.split(' -> ');
            return segments.length === 2;
        });
    }

    handleTableSelect(tableName) {
        if (this.selectedTables.has(tableName)) this.selectedTables.delete(tableName);
        else this.selectedTables.add(tableName);
        
        this.obj.graph.refresh();
    }

    syncSqlEditor() {
        const textarea = this.obj.container.querySelector('#sqlTextarea');
        if (!textarea) return;

        const selected = Array.from(this.selectedTables);

        if (selected.length === 0) {
            textarea.value = `-- e2e-Data Query Builder\n-- Select tables in the diagram to auto-generate JOINs.`;
            return;
        }

        const baseTable = selected[0];
        const joins = [];
        const processedTables = new Set([baseTable]);
        const relations = Array.from(this.relationRegistry.values());

        selected.slice(1).forEach(currentTable => {
            const connection = relations.find(rel => 
                (rel.sourceTable === currentTable && processedTables.has(rel.targetTable)) || 
                (rel.targetTable === currentTable && processedTables.has(rel.sourceTable))
            );

            if (connection) {
                const leftTable = connection.sourceTable;
                const leftCol = connection.sourceColumn;
                const rightTable = connection.targetTable;
                const rightCol = connection.targetColumn;

                joins.push(`JOIN ${currentTable} ON ${leftTable}.${leftCol} = ${rightTable}.${rightCol}`);
                processedTables.add(currentTable);
            } else 
                joins.push(`, ${currentTable} -- Note: Cross-join or missing relationship metadata`);
            
        });

        const sql = [
            `-- Auto-generated Business Query`,
            `SELECT`,
            `    *`,
            `FROM`,
            `    ${baseTable}`,
            `    ${joins.join('\n    ')};`
        ].join('\n');

        textarea.value = sql;
    }

}