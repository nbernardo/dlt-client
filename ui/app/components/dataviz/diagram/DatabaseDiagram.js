import { sleepForSec } from "../../../../@still/component/manager/timer.js";
import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { UUIDUtil } from "../../../../@still/util/UUIDUtil.js";
import { BIUserInterfaceComponent } from "../bi/main/BIUserInterfaceComponent.js";
import { DataQualityController } from "../controllers/DataQualityController.js";
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
	 * @Controller @Path components/dataviz/controllers/
	 * @type { DBDiagramController }
	*/ controller;

    /**
     * @Inject @Path components/dataviz/controllers/
     * @type { DataQualityController }
     */ dqController;

    secretList;
    pipelineTablesList;
    totalTablesAdded = 0;
    existingModels = [];
    pipelinePlans = [];
    saveBtnLabel = 'Save';

    /** @Prop */ bridgeOverlays;

    stOnRender() { 
        if (!G6.registerNode.isDbRegistered) {
            DBDiagramController.initCustomDBNode(); 
            G6.registerNode.isDbRegistered = true;
        }
    }
    
    stAfterInit() { 
        this.bridgeOverlays = {};
        this.container = document.getElementById(this.uniqueId);
        this.controller.on('load', async () => {
            this.controller.obj = this;
            this.init();
            this.controller.bindToolbar();
            await this.controller.loadCodeEditor();
        });
        this.dqController.on('load', () => this.dqController.dbDiagramObj = this);
        this.existingModels = this.$parent.domainPipelinesList.value || [];
        this.$parent.domainPipelinesList.onChange((value) => this.dbDiagramProxy.existingModels = value);
    }

    initGraph() {
        const contnr = this.container.querySelector('#mountNode');
        if (!contnr) return null;
        const width = contnr.scrollWidth, height = contnr.scrollHeight || 600;

		const graph = new G6.TreeGraph({
			container: 'mountNode', width, height, animate: true, fitView: false, fitCenter: true, cursor: 'grab', renderer: 'svg',
            modes: {
                default: [
                    'drag-canvas', 'zoom-canvas', 
                    { type: 'create-edge', trigger: 'drag', edgeConfig: { type: 'cubic-horizontal', style: { stroke: '#1890ff', lineDash: [5, 5] } }}
                ]
            },
			layout: {
				type: 'compactBox', direction: 'LR', getId: (d) => d.id, getHeight: () => 20, getVGap: () => 15, 
				getWidth: (d) => DBDiagramController.calculateTextWidth(d.label) + 20, getHGap: () => 100,
			},
			defaultNode: { type: 'db-table' }, defaultEdge: { type: 'cubic-horizontal', style: { stroke: '#A3B1BF', lineWidth: 1 } },
		});

        if (typeof window !== 'undefined') {
            window.addEventListener('resize', () => {
                if (!graph || graph.get('destroyed')) return;
                graph.changeSize(contnr.scrollWidth, contnr.scrollHeight);
            });
        }

		const canvasEl = graph.get('canvas').get('el');
		graph.on('canvas:mouseenter', () => canvasEl.style.cursor = 'grab');	
		graph.on('canvas:dragstart', () => canvasEl.style.cursor = 'grabbing');
		graph.on('canvas:dragend', () => canvasEl.style.cursor = 'grab');

        graph.on('aftercreateedge', (e) => {
            const { edge } = e;
            const sourceItem = edge.getSource(),  targetItem = edge.getTarget();
            const sourceModel = sourceItem.getModel(), targetModel = targetItem.getModel();

            graph.updateItem(edge, { type: 'cubic-horizontal', style: { lineDash: [4, 4], stroke: '#1890ff', lineWidth: 2 }});

            const parseCols = (raw) => {
                if (!raw) return [];
                if (Array.isArray(raw)) return raw.map(c => c.trim().split(' ')[0]);
                return raw.split(',').map(c => c.trim().split(' ')[0]).filter(Boolean);
            };

            const sourceCols = parseCols(sourceModel.columns), targetCols = parseCols(targetModel.columns);
            const bridgeId = `bridge-${sourceModel.id}-${targetModel.id}`;

            this.controller.addManualRelation(
                `${sourceModel.label}.${sourceCols[0] || sourceModel.label}`,
                `${targetModel.label}.${targetCols[0] || targetModel.label}`
            );

            this._mountBridgeOverlay(graph, { bridgeId, sourceModel, targetModel, sourceCols, targetCols });
        });

        return graph;
    }

    _mountBridgeOverlay(graph, { bridgeId, sourceModel, targetModel, sourceCols, targetCols }) {
        const mountNode = this.container.querySelector('#mountNode');
        if (!mountNode) return;

        this._removeBridgeOverlay(graph, bridgeId);
        mountNode.style.position = 'relative';

        const overlay = document.createElement('div');
        overlay.id = bridgeId;
        Object.assign(overlay.style, {
            position: 'absolute', width: '160px', background: '#ffffff', border: '2px solid #1890ff', borderRadius: '6px',
            padding: '8px 10px', boxShadow: '0 4px 12px rgba(0,0,0,0.18)', zIndex: '200', fontSize: '11px', pointerEvents: 'all',
        });

        overlay.innerHTML = `
            <div style="font-weight:bold;color:#1890ff;margin-bottom:6px;display:flex;justify-content:space-between;align-items:center;">
                <span>⚡ Join</span> <span class="bridge-close" style="cursor:pointer;color:#999;font-size:13px;line-height:1;">✕</span>
            </div>
            <div style="margin-bottom:3px;color:#555;font-size:10px;">${sourceModel.label}</div>
            <select class="s-sel" style="width:100%;margin-bottom:6px;font-size:10px;padding:2px 4px;">
                ${sourceCols.map(c => `<option value="${c}">${c}</option>`).join('')}
            </select>
            <div style="margin-bottom:3px;color:#555;font-size:10px;">${targetModel.label}</div>
            <select class="t-sel" style="width:100%;font-size:10px;padding:2px 4px;">
                ${targetCols.map(c => `<option value="${c}">${c}</option>`).join('')}
            </select>
        `;

        overlay.querySelector('.bridge-close').addEventListener('click', () => this._removeBridgeOverlay(graph, bridgeId));

        const syncRelation = () => {
            const sCol = overlay.querySelector('.s-sel').value, tCol = overlay.querySelector('.t-sel').value;
            this.controller.addManualRelation(`${sourceModel.tableName}.${sCol}`,`${targetModel.tableName}.${tCol}`);
        };
        overlay.querySelector('.s-sel').addEventListener('change', syncRelation);
        overlay.querySelector('.t-sel').addEventListener('change', syncRelation);
        mountNode.appendChild(overlay);

        const rpostion = () => {
            const sNode = graph.findById(sourceModel.id), tNode = graph.findById(targetModel.id);
            if (!sNode || !tNode) return;

            const sM = sNode.getModel(), tM = tNode.getModel();
            const sPoint = graph.getCanvasByPoint(sM.x, sM.y), tPoint = graph.getCanvasByPoint(tM.x, tM.y);
            const midX = (sPoint.x + tPoint.x) / 2, midY = (sPoint.y + tPoint.y) / 2;
            overlay.style.left = `${midX - 80}px`, overlay.style.top  = `${midY + 16}px`; 
        };
        rpostion();
        graph.on('afterlayout', rpostion), graph.on('viewportchange', rpostion), graph.on('wheel', rpostion), graph.on('canvas:drag', rpostion);
        this.bridgeOverlays[bridgeId] = { overlay, rpostion };
    }

    _removeBridgeOverlay(graph, bridgeId) {
        if(this.bridgeOverlays[bridgeId]){
            const { overlay, rpostion } = this.bridgeOverlays[bridgeId];
            if (!entry) return;
            overlay.remove();
            graph.off('afterlayout', rpostion), graph.off('viewportchange', rpostion), graph.off('wheel', rpostion), graph.off('canvas:drag', rpostion);
            delete this.bridgeOverlays[bridgeId];
            this.controller.relationRegistry.delete(`manual_${bridgeId}`);
        }
    }

	init() {
        this.graph = this.initGraph();
        if (!this.graph) return;
        this.graph.data({ id: 'root', label: 'e2e-Data Platform', isRoot: true, children: [] });
        this.graph.render();
        this.controller.setGraphOnClickEvt(this.graph);
    }

    async updateGraphData(summaryRows, tableName) {
        this.graph.clear();
        if(summaryRows){
            if (!this.graph) return setTimeout(() => this.updateGraphData(summaryRows), 50);
            if (!summaryRows || !summaryRows?.tables || summaryRows?.tables?.length === 0) return;
    
            const container = this.container.querySelector('#mountNode');
            if (container) this.graph.changeSize(container.scrollWidth, container.scrollHeight);
    
            this.controller.compileRelations(summaryRows.relations);
            this.controller.listToTree(summaryRows.tables, tableName, summaryRows.relations, 0);
            this.graph.data(this.controller.anchorNode);
            this.graph.render();
        
            this.graph.fitView([40, 40, 40, 40]);
            this.graph.zoomTo(2.0, { x: this.graph.getWidth() / 2, y: this.graph.getHeight() / 2 });

            const width = this.graph.getWidth();
            this.graph.translate(-(width / 5), 0);

            setTimeout(() => {
                const item = this.graph.findById(this.controller.anchorNode.id);
                if (item)
                    this.graph.emit('node:click', { item, target: item.getContainer().get('children')[0] });
            }, 100);
        }
    }

}