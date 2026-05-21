import { BaseController } from "../../../../@still/component/super/service/BaseController.js";
import { DatabaseDiagram } from "../diagram/DatabaseDiagram.js";

export class DataQualityController extends BaseController {

    /** @returns { DataQualityController } */
    static fromContext(){ return DataQualityController.get() }

    // Rule Definitions
    dqRules = {
        TRIM: (f) => `TRIM(${f})`, 
        LOWER: (f) => `LOWER(${f})`, 
        UPPER: (f) => `UPPER(${f})`,
        COALESCE: (f) => `COALESCE(${f}, '')`,
        DISTINCT: (f) => `DISTINCT ${f}`, 
        NULL_TO_ZERO: (f) => `COALESCE(${f}, 0)`, 
        TO_TEXT: (f) => `CAST(${f} AS TEXT)`, 
        TO_INT: (f) => `CAST(${f} AS INTEGER)`,
        DATE_FORMAT: (f, fmt) => `TO_CHAR(${f}, '${fmt}')`,
        LPAD: (f, len) => `LPAD(${f}::text, ${len || 10}, '0')`,
        RPAD: (f, len) => `RPAD(${f}::text, ${len || 10}, '0')`
    };

    /** @type { Map<string, object> } */ 
    columnQualityRules = new Map();

    /** @type { DatabaseDiagram } */ dbDiagramObj;

    openQualityMenu(event, fieldPath, tableName, moduleName) {
        event.stopPropagation();
        const rule = prompt("DQ Rule: 1:TRIM, 2:UPPER, 3:LOWER, 4:COALESCE, Clear: Enter");
        const rulesMap = { '1': 'TRIM', '2': 'UPPER', '3': 'LOWER', '4': 'COALESCE' };

        if (rulesMap[rule]) {
            this.columnQualityRules.set(fieldPath, rulesMap[rule]);
        } else {
            this.columnQualityRules.delete(fieldPath);
        }

        this.dbDiagramObj.controller.syncSqlEditor();
        this.dbDiagramObj.controller.toggleTableFields(tableName, moduleName);
    }

    /**
     * @param { Set<String> } selectedFieldsSet 
     */
    handleValidField(selectedFieldsSet){

        return Array.from(selectedFieldsSet)
            .filter(f => processed.has(f.split('.')[0]))
            .map(f => {
                const ruleKey = this.columnQualityRules.get(f);
                const [tbl, col] = f.split('.');
                const cleanCol = col.replace(' (PK)', ''); // Strip PK tag for SQL
                const fullCol = `${tbl}.${cleanCol}`;

                if (ruleKey && DataQualityController.dqRules[ruleKey]) {
                    // Apply the transformation and alias it back to the original name
                    return `${DataQualityController.dqRules[ruleKey](fullCol)} AS ${cleanCol}`;
                }
                return fullCol;
            });        
    }

    toggleDQMenu(event, fieldPath) {
        event.stopPropagation();
        const menuId = `dq-menu-${fieldPath.replace(/[^a-zA-Z0-9]/g, '_')}`;
        const menu = document.getElementById(menuId);
        
        document.querySelectorAll('.dq-dropdown').forEach(el => el !== menu ? el.style.display = 'none' : '');
        menu.style.display = menu.style.display === 'none' ? 'block' : 'none';

        const closeMenu = () => {
            menu.style.display = 'none';
            document.removeEventListener('click', closeMenu);
        };
        setTimeout(() => document.addEventListener('click', closeMenu), 10);
    }

    applyRule(fieldPath, rule, tableName, moduleName) {
        if (rule) this.columnQualityRules.set(fieldPath, rule);
        else this.columnQualityRules.delete(fieldPath);

        this.dbDiagramObj.controller.syncSqlEditor();
        //this.dbDiagramObj.controller.toggleTableFields(tableName, moduleName);
    }

    getFormattedField(fullCol, cleanField, ruleEntry) {
        if (!ruleEntry) return null;
        
        const ruleKey = typeof ruleEntry === 'object' ? ruleEntry.rule : ruleEntry;
        const params = typeof ruleEntry === 'object' ? ruleEntry.params : null;
        
        if (this.dqRules[ruleKey]) {
            const transformed = params ? this.dqRules[ruleKey](fullCol, params) : this.dqRules[ruleKey](fullCol);
            return `${transformed} AS ${cleanField}`;
        }
        return null;
    }

    addStandaloneTable(tableName) {
        const graph = this.dbDiagramObj.graph;
        if (!graph || graph.get('destroyed')) return;

        if (graph.findById(tableName))
            return console.warn(`Table ${tableName} is already present on the canvas.`);

        const rawFields = this.dbDiagramObj.controller.pipelineTableFields.get(tableName) || [];
        const columns = Array.isArray(rawFields) ? rawFields.join(', ') : rawFields;

        const mountNode = this.dbDiagramObj.container.querySelector('#mountNode');
        const containerWidth  = mountNode?.scrollWidth  || 800;
        const containerHeight = mountNode?.scrollHeight || 600;
        const center = graph.getPointByCanvas(containerWidth / 2, containerHeight / 2);
        const offset = graph.findAll('node', n => n.getModel().isStandalone).length * 60;

        graph.addItem('node', {
            id: tableName, label: tableName, tableName: tableName, columns, isStandalone: true,
            isExternal: true, level: 2, depth: 1, x: center.x + offset, y: center.y + offset,
        });

        graph.get('canvas').draw();
    }

    connectModeActive = false;

    toggleConnectMode(btn) {
        const graph = this.dbDiagramObj.graph;
        this.connectModeActive = !this.connectModeActive;

        if (this.connectModeActive) {
            graph.setMode('connect');
            btn.classList.add('active');
            btn.textContent = '✕ Disable table connect';
            // Show visual hint on all standalone nodes
            graph.findAll('node', n => n.getModel().isStandalone).forEach(node => {
                graph.updateItem(node, { connectMode: true });
            });
        } else {
            graph.setMode('default');
            btn.classList.remove('active');
            btn.textContent = '⊟ Enable table connect';
            graph.findAll('node', n => n.getModel().isStandalone).forEach(node => {
                graph.updateItem(node, { connectMode: false });
            });
        }
    }

}