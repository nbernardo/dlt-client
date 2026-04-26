import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { BIService } from "../../../services/BIService.js";

/** This component used the G6 library (https://g6.antv.antgroup.com/en/manual/introduction) which
* is being dynamically imported in the app-setup.js, especially because of the heaviness of the
* library, importing it directly here makes things to delay even more or the component not to load */
export class DatabaseDiagram extends ViewComponent {
    isPublic = true;

    /** @Prop */ graph;

	/** @Prop */ initCount = 0;

    calculateTextWidth(text, font = '8px Arial') {
        const context = document.createElement('canvas').getContext('2d');
        context.font = font;
        return context.measureText(text || '').width;
    }

    listToTree(data, moduleName) {
        const map = {};
        const folderChildren = [];
        const blacklist = ['base_', 'portal_', 'l10n_', 'change_password_', 'wizard_ir_', 'res_users', 'res_groups', 'res_partner', 'mail_'];
        
        data.forEach(row => {
            const [level, parentName, tableName, path] = row;
            if (!path) return;

            const isBlacklisted = blacklist.some(p => tableName.startsWith(p));
            if (isBlacklisted) return;

            const id = path.replace(/^[a-z0-9_]+/i, moduleName);            
            const isExternal = !tableName.toLowerCase().startsWith(moduleName.toLowerCase());

            map[id] = { id, label: tableName, level: level, isExternal: isExternal, children: [], collapsed: true };
        });

        Object.values(map).forEach(node => {
            const parts = node.id.split(' -> ');
            const isModuleTable = !node.isExternal;
            
            if (parts.length === 1 || (parts.length === 2 && isModuleTable)) {
                folderChildren.push(node);
            } else {
                const parentId = parts.slice(0, -1).join(' -> ');
                if (map[parentId]) {
                    map[parentId].children.push(node);
                } else if (parts.length === 2) {
                    const rootId = parts[0]; 
                    if (map[rootId]) map[rootId].children.push(node);
                }
            }
        });
        return folderChildren;
    }

    stOnRender() { 
        if (!G6.registerNode.isDbRegistered) {
            this.initCustomDBNode(); 
            G6.registerNode.isDbRegistered = true;
        }
    }
    
    stAfterInit() { this.init(); }

    initCustomDBNode() {
        const self = this;
        G6.registerNode('db-table', {
            draw(cfg, group) {
                const { label = '', isRoot, isExternal } = cfg;
                const fontSize = 8, height = 20;
                const width = self.calculateTextWidth(label, `${fontSize}px Arial`) + 16;

                let fill = cfg.level === 1 ? '#e6f7ff' : '#f0f5ff';
                let stroke = cfg.level === 1 ? '#91d5ff' : '#adc6ff';
                let textColor = isRoot ? '#ffffff' : '#000000';
                let lineDash = null;

                if (isRoot) { 
                    fill = '#1d39c4', stroke = '#002329'; 
                } else if (isExternal) {
                    fill = '#ffffff', stroke = '#ffa39e', lineDash = [3, 2];
                }

                const keyShape = group.addShape('rect', {
                    attrs: { 
                        x: -width / 2, y: -height / 2, width, height, fill, 
                        stroke, lineWidth: 1, radius: 2, lineDash: lineDash 
                    },
                    name: 'table-container',
                });

                group.addShape('text', {
                    attrs: { 
                        x: 0, y: 0, textAlign: 'center', textBaseline: 'middle', text: label, 
                        fill: textColor, fontSize, fontWeight: isRoot ? 'bold' : 'normal' 
                    },
                    name: 'table-label',
                });
                return keyShape;
            }
        });
    }

    initGraph() {
        const container = document.getElementById('mountNode');
        if (!container) return null;

        const graph = new G6.TreeGraph({
            container: 'mountNode', width: container.scrollWidth, height: container.scrollHeight || 600,
            fitView: false, fitCenter: true, modes: { default: ['drag-canvas', 'zoom-canvas'] },
            layout: {
                type: 'compactBox', direction: 'LR', getId: (d) => d.id, getHeight: () => 20, 
				getWidth: (d) => this.calculateTextWidth(d.label) + 20, getVGap: () => 10, getHGap: () => 60,
            },
            animate: true,
            defaultNode: { type: 'db-table' },
            defaultEdge: { type: 'cubic-horizontal', style: { stroke: '#A3B1BF', lineWidth: 1 } },
        });

        if (typeof window !== 'undefined') {
            window.addEventListener('resize', () => {
                if (!graph || graph.get('destroyed')) return;
                graph.changeSize(container.scrollWidth, container.scrollHeight);
            });
        }
        return graph;
    }

    init() {
        this.graph = this.initGraph();
        if (!this.graph) return;

        this.graph.data({ id: 'root', label: 'e2e-Data Platform', isRoot: true, children: [] });
        this.graph.render();

        this.graph.on('node:click', async (e) => {
            const { item } = e;
            const model = item.getModel();

            if (model.children && model.children.length > 0) {
                this.graph.updateItem(item, { collapsed: !model.collapsed });
                return this.graph.layout(); 
            }

            if (model.id.startsWith('folder:')) {
                const moduleName = model.label; 
                this.graph.updateItem(item, { label: `${moduleName} (Loading...)` });

                try {
                    const result = await BIService.getTablesWhenOdoo(moduleName.toLowerCase());
                    const children = this.listToTree(result, moduleName);
                    this.graph.updateItem(item, { label: moduleName, children: children, collapsed: false });
                    this.graph.layout(); 
                } catch (err) {
                    this.graph.updateItem(item, { label: moduleName });
                }
            }
        });
    }

    updateGraphData(summaryRows) {
        if (!this.graph) 
            return setTimeout(() => this.updateGraphData(summaryRows), 50);

        if (!summaryRows || summaryRows.length === 0) return;

        const container = document.getElementById('mountNode');
        if (container) this.graph.changeSize(container.scrollWidth, container.scrollHeight);
        
        const moduleNodes = summaryRows.map(row => ({
            id: `folder:${row[2].toUpperCase()}`, label: row[2].toUpperCase(), level: 1, children: [], collapsed: true
        }));

        this.graph.data({ id: 'root', label: 'e2e-Data Platform', isRoot: true, children: moduleNodes });
        this.graph.render();
        this.graph.fitView(40);
    }
}