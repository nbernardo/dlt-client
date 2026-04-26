import { sleepForSec } from "../../../../@still/component/manager/timer.js";
import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { UUIDUtil } from "../../../../@still/util/UUIDUtil.js";
import { BIService } from "../../../services/BIService.js";
import { DBDiagramController } from "./controller/DBDiagramController.js";

/** This component used the G6 library (https://g6.antv.antgroup.com/en/manual/introduction) which
* is being dynamically imported in the app-setup.js, especially because of the heaviness of the
* library, importing it directly here makes things to delay even more or the component not to load */
export class DatabaseDiagram extends ViewComponent {
    isPublic = true;

    /** @Prop */ graph;

    /** @Prop */ uniqueId = `_${UUIDUtil.newId()}`;

    /** @Prop @type { HTMLElement } */ container;

	/** @Prop */ initCount = 0;

	/**
	 * @Controller
	 * @Path components/dataviz/diagram/controller/
	 * @type { DBDiagramController }
	 */ controller;



    stOnRender() { 
        if (!G6.registerNode.isDbRegistered) {
            DBDiagramController.initCustomDBNode(); 
            G6.registerNode.isDbRegistered = true;
        }
    }
    
    stAfterInit() { 
		this.container = document.getElementById(this.uniqueId);
		this.controller.on('load', () => {
			this.controller.obj = this;
			this.init();
			this.controller.bindToolbar();
		});
	}

    initGraph() {
        const container = this.container.querySelector('#mountNode');
        if (!container) return null;

        const graph = new G6.TreeGraph({
            container: 'mountNode', width: container.scrollWidth, height: container.scrollHeight || 600,
            fitView: false, fitCenter: true, modes: { default: ['drag-canvas', 'zoom-canvas'] },
            layout: {
                type: 'compactBox', direction: 'LR', getId: (d) => d.id, getHeight: () => 20, 
				getWidth: (d) => DBDiagramController.calculateTextWidth(d.label) + 20, getVGap: () => 10, getHGap: () => 60,
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

        // Initialize with the root node for the e2e-Data Platform
        this.graph.data({ 
            id: 'root', 
            label: 'e2e-Data Platform', 
            isRoot: true, 
            children: [] 
        });
        this.graph.render();

        // Single event listener to manage all node interactions
        this.graph.on('node:click', async (e) => {
            const { item, target } = e;
            const model = item.getModel();
            const shapeName = target.get('name');

            /** * 1. HANDLE TABLE SELECTION (SQL BUILDER)
             * Checks if the click was on the green/red icon circle or the +/- text
             */
            if (shapeName === 'select-icon-bg' || shapeName === 'select-icon-text') {
                // Controller manages the Set of selected tables
                this.controller.handleTableSelect(model.label);
                
                // Update the view state for this specific node
                this.graph.updateItem(item, { isSelected: this.controller.selectedTables.has(model.label)});

				//await this.onFolderExpand(moduleName, item);
                
                // Return early to prevent triggering expansion logic
                return;
            }

            /** * 2. HANDLE TREE EXPANSION / COLLAPSE
             * If the node already has children loaded, just toggle visibility
             */
            if (model.children && model.children.length > 0) {
                this.graph.updateItem(item, { 
                    collapsed: !model.collapsed 
                });
                return this.graph.layout(); 
            }

            /** * 3. HANDLE DYNAMIC LOADING (ODOO MODULES)
             * If it's a folder node and has no children, fetch data from BIService
             */
            if (model.id.startsWith('folder:')) {
                const moduleName = model.label; 
                this.graph.updateItem(item, { label: `${moduleName} (Loading...)` });

                try {
                    // Call service layer for Odoo metadata
                    const result = await BIService.getTablesWhenOdoo(moduleName.toLowerCase());
                    
                    // Controller handles the heavy lifting of list-to-tree conversion
                    const children = this.controller.listToTree(result.tables, moduleName);

                    setTimeout(() => this.controller.compileRelations(result.relations));
                    
                    this.graph.updateItem(item, { 
                        label: moduleName, 
                        children: children, 
                        collapsed: false 
                    });
                    
                    this.graph.layout(); 
                } catch (err) {
                    console.error('Failed to load module tables:', err);
                    this.graph.updateItem(item, { label: moduleName });
                }
            }
        });
    }

    async updateGraphData(summaryRows) {
        if (!this.graph) return setTimeout(() => this.updateGraphData(summaryRows), 50);

        if (!summaryRows || summaryRows.length === 0) return;

        const container = this.container.querySelector('#mountNode');
        if (container) this.graph.changeSize(container.scrollWidth, container.scrollHeight);
        
        const moduleNodes = summaryRows.map(row => ({
            id: `folder:${row[2].toUpperCase()}`, label: row[2].toUpperCase(), level: 1, children: [], collapsed: true
        }));

        this.graph.data({ id: 'root', label: 'e2e-Data Platform', isRoot: true, children: moduleNodes });
        this.graph.render();
        this.graph.fitView(40);

		if(this.initCount == 0){
			await sleepForSec(200);
			this.initCount++, await this.updateGraphData(summaryRows);
		}

    }

	// DatabaseDiagram.js
	async onFolderExpand(moduleName, item) {
		try {
			const result = await BIService.getTablesWhenOdoo(moduleName);
			// If result is an object with a 'rows' or 'data' property, use that:
			const rows = Array.isArray(result) ? result : result.data; 
			
			this.compileRelations(rows);            
			const children = this.controller.listToTree(rows, moduleName);
			
			this.graph.updateItem(item, { children, collapsed: false });
			this.graph.layout(); 
		} catch (err) {
			console.error('Failed to load module tables:', err);
		}
	}
}