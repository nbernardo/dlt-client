import { sleepForSec } from "../../../../@still/component/manager/timer.js";
import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { UUIDUtil } from "../../../../@still/util/UUIDUtil.js";
import { BIUserInterfaceComponent } from "../bi/main/BIUserInterfaceComponent.js";
import { DBDiagramController } from "../controllers/DBDiagramController.js";

/** This component used the G6 library (https://g6.antv.antgroup.com/en/manual/introduction) which
* is being dynamically imported in the BIUserInterfaceComponent.js, especially because of the heaviness of the
* library, importing it directly here makes things to delay even more or the component not to load */
export class DatabaseDiagram extends ViewComponent {
    isPublic = true;

    /** @Prop */ graph;

    /** @Prop */ uniqueId = `_${UUIDUtil.newId()}`;

    /** @Prop @type { HTMLElement } */ container;

    /** @Prop @type { BIUserInterfaceComponent } */ $parent;

	/** @Prop */ initCount = 0;

	/** @Prop */ showDiagram = true;

	/**
	 * @Controller
	 * @Path components/dataviz/controllers/
	 * @type { DBDiagramController }
	*/ controller;

    secretList;

    stOnRender() { 
        if (!G6.registerNode.isDbRegistered) {
            DBDiagramController.initCustomDBNode(); 
            G6.registerNode.isDbRegistered = true;
        }
    }
    
    stAfterInit() { 
		this.container = document.getElementById(this.uniqueId);
		this.controller.on('load', async () => {
			this.controller.obj = this;
			this.init();
			this.controller.bindToolbar();
            await this.controller.loadCodeEditor();
		});
	}

    initGraph() {
        const container = this.container.querySelector('#mountNode');
        if (!container) return null;

		const graph = new G6.TreeGraph({
			container: 'mountNode',  width: container.scrollWidth, animate: true,
			height: container.scrollHeight || 600, fitView: false, fitCenter: true, 
			container: 'mountNode', cursor: 'grab', 
			modes: { default: [{ type: 'drag-canvas', delegateStyle: { cursor: 'grabbing' }, }, 'zoom-canvas'] },
			layout: {
				type: 'compactBox', direction: 'LR', getId: (d) => d.id, getHeight: () => 20, getVGap: () => 15, 
				getWidth: (d) => DBDiagramController.calculateTextWidth(d.label) + 20, getHGap: () => 100,
			},
			defaultNode: { type: 'db-table' }, defaultEdge: { type: 'cubic-horizontal', style: { stroke: '#A3B1BF', lineWidth: 1 } },
		});

        if (typeof window !== 'undefined') {
            window.addEventListener('resize', () => {
                if (!graph || graph.get('destroyed')) return;
                graph.changeSize(container.scrollWidth, container.scrollHeight);
            });
        }

		const canvasEl = graph.get('canvas').get('el');
		graph.on('canvas:mouseenter', () => canvasEl.style.cursor = 'grab');	
		graph.on('canvas:dragstart', () => canvasEl.style.cursor = 'grabbing');
		graph.on('canvas:dragend', () => canvasEl.style.cursor = 'grab');

        return graph;
    }

	init() {
        this.graph = this.initGraph();
        if (!this.graph) return;

        // Initialize with the root node for the e2e-Data Platform
        this.graph.data({ id: 'root', label: 'e2e-Data Platform', isRoot: true, children: [] });
        this.graph.render();
        this.controller.setGraphOnClickEvt(this.graph);
    }

    async updateGraphData(summaryRows) {
        this.graph.clear();
        if(summaryRows){
            if (!this.graph) return setTimeout(() => this.updateGraphData(summaryRows), 50);
            if (!summaryRows || summaryRows.length === 0) return;
    
            const container = this.container.querySelector('#mountNode');
            if (container) this.graph.changeSize(container.scrollWidth, container.scrollHeight);
            
            const moduleNodes = summaryRows.map(row => ({
                id: `folder:${row[2].toUpperCase()}`, label: row[2].toUpperCase(), level: 1, children: [], collapsed: true
            }));
    
            this.graph.data({ id: 'root', label: 'Odoo modules', isRoot: true, children: moduleNodes });
            this.graph.render();
            this.graph.fitView(40);
    
            if(this.initCount == 0){
                await sleepForSec(200);
                this.initCount++, await this.updateGraphData(summaryRows);
            }
        }
    }

}