import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";

export class DatabaseDiagram extends ViewComponent {
	/**
	 * This component used the G6 library (https://g6.antv.antgroup.com/en/manual/introduction) which
	 * is being dynamically imported in the app-setup.js, especially because of the heaviness of the
	 * library, importing it directly here makes things to delay even more or the component not to load
	 */

	isPublic = true;

	/** @Prop */ graph;

	async stOnRender(){ this.initCustomDBNode(); }

	stAfterInit(){
		this.init();
	}

	wrapperTableNode(group, width, height, color){
		return group.addShape('rect', {
			attrs: { x: 0, y: 0, width, height, fill: '#ffffff', stroke: color, lineWidth: 1, radius: 6, cursor: 'move' },
			name: 'table-container', draggable: true,
		});
	}

	addTableHeader(group, width, headerHeight, color){
		group.addShape('rect', {
			attrs: { x: 0, y: 0, width, height: headerHeight, fill: color, radius: [6, 6, 0, 0], cursor: 'move' },
			name: 'header', draggable: true,
		});
	}

	addHeaderText(group, name, width, headerHeight){
		group.addShape('text', {
			attrs: {
				x: width / 2, y: headerHeight / 2, textAlign: 'center', textBaseline: 'middle',
				text: name, fill: '#ffffff', fontWeight: 'bold', fontSize: 12, cursor: 'move',
			},
			name: 'header-text', draggable: true,
		});
	}

	addTableRow(group, col, yOffset, width, rowHeight){
		// This adds the row with column name
		group.addShape('rect', {
			attrs: {  x: 0, y: yOffset, width, height: rowHeight,  fill: '#fff',  cursor: 'move'  },
			name: `row-${col.name}`, draggable: true
		});
		// This adds the highlighter that takes place 
		// when we hover in the tables relationship arrow
		group.addShape('text', {
			attrs: { x: 10, y: yOffset + rowHeight / 2, textBaseline: 'middle', text: col.name, fill: '#333', fontSize: 11, cursor: 'move' },
			name: `text-${col.name}`, draggable: true
		});
	}

	initCustomDBNode(){
		const self = this;
		G6.registerNode('db-table', {
			draw(cfg, group) {
				const { name, columns, collapsed, color = '#5B8FF9' } = cfg;
				const width = 200, headerHeight = 35, rowHeight = 25;
				const height = collapsed ? headerHeight : headerHeight + ((columns || []).length * rowHeight);
				const keyShape = self.wrapperTableNode(group, width, height, color);

				self.addTableHeader(group, width, headerHeight, color);
				self.addHeaderText(group, name, width, headerHeight);

				(columns || []).forEach((col, i) => self.addTableRow(group, col, (headerHeight + (i * rowHeight)), width, rowHeight));

				return keyShape;
			},
			getAnchorPoints() { return [[0, 0.5], [1, 0.5]]; }
		}, 'rect');
	}

	initGraph() {
		const container = document.getElementById('mountNode');
		if (!container) return null;

		const graphInstance = new G6.Graph({
			container: 'mountNode',
			width: container.clientWidth || 800, 
			height: container.clientHeight || 600,
			fitView: true,
			fitViewPadding: 50,
			modes: {
				default: ['drag-canvas', 'zoom-canvas', 'drag-node'],
			},
			layout: {
				type: 'gForce',
				gravity: 10,
				edgeStrength: 200,
				nodeStrength: 1000,
				preventOverlap: true,
				workerEnabled: true, 
				gpuEnabled: true 
			},
			defaultNode: {
				type: 'db-table',
			}
		});

		// Handle window resizing
		window.addEventListener('resize', () => {
			if (!graphInstance || graphInstance.get('destroyed')) return;
			graphInstance.changeSize(container.clientWidth, container.clientHeight);
		});

		// RETURN the instance so this.graph is defined
		return graphInstance;
	}

	toggleFieldHighlight(graph, edgeItem, isHovering){
		const model = edgeItem.getModel();
		const sourceNode = graph.findById(model.source), targetNode = graph.findById(model.target);

		const highlight = (node, fieldName) => {
			if (!node || !fieldName) return;
			const group = node.getContainer();
			const row = group.find(el => el.get('name') === `row-${fieldName}`);
			if (row) {
				row.attr('fill', isHovering ? '#e6f7ff' : '#ffffff');
				row.attr('stroke', isHovering ? '#1890ff' : 'transparent');
			}
		};

		highlight(sourceNode, model.sourceField);
		highlight(targetNode, model.targetField);

		graph.updateItem(edgeItem, { style: { stroke: isHovering ? '#1890ff' : '#A3B1BF', lineWidth: isHovering ? 3 : 2 } });
	};

	init(){

		const data = {
			nodes: [
				{ id: 't1', name: 'Users', type: 'db-table', x: 100, y: 100, columns: [{name: 'id'}, {name: 'name'}] },
				{ id: 't2', name: 'Posts', type: 'db-table', x: 450, y: 100, columns: [{name: 'id'}, {name: 'user_id'}] },
				{ id: 't3', name: 'Peoples', type: 'db-table', x: 450, y: 300, columns: [{name: 'id'}, {name: 'user_id'}] }
			],
			edges: [
				{ 
				source: 't1', target: 't2', label: '1:N', sourceField: 'id',  targetField: 'user_id', 
					labelCfg: {
						autoRotate: false,
						style: {
							fill: '#333', fontSize: 11, fontWeight: 'bold',
							background: { fill: '#ffffff', padding: [2, 4, 2, 4], radius: 2, stroke: '#A3B1BF', lineWidth: 1 }
						}
					}
				},
				{ 
				source: 't1',  target: 't3', label: '1:N', sourceField: 'id', targetField: 'user_id', 
					labelCfg: {
						autoRotate: false, 
						style: {
							fill: '#333', fontSize: 11, fontWeight: 'bold',
							background: { fill: '#ffffff', padding: [2, 4, 2, 4], radius: 2, stroke: '#A3B1BF', lineWidth: 1 }
						}
					}
				}
			]
		};

		this.graph = this.initGraph();
		this.graph.on('edge:mouseenter', (evt) => this.toggleFieldHighlight(this.graph, evt.item, true));
		this.graph.on('edge:mouseleave', (evt) => this.toggleFieldHighlight(this.graph, evt.item, false));

		this.graph.data(data); this.graph.render();
	}

	updateGraphData(dbRows) {
		if (!dbRows || !Array.isArray(dbRows)) return;

		if (!this.nodeRegistered) {
			this.initCustomDBNode();
			this.nodeRegistered = true; 
		}

		const nodes = dbRows.map(row => {
			// row[0] is the model name from your SQL query (e.g., 'account.move')
			const modelName = row[0] || 'Unknown Model'; 
			const prefix = modelName.split('.')[0];
			
			const colors = {
				sale: '#3b82f6',
				account: '#10b981',
				stock: '#f59e0b',
				product: '#ef4444',
				res: '#8b5cf6'
			};

			return {
				id: modelName,        // Used for edge connections
				name: modelName,      // Used by your custom node 'draw' function
				type: 'db-table',
				table_name: modelName.replace(/\./g, '_'),
				color: colors[prefix] || '#94a3b8', // Matches your draw() parameter
				// Initial spread to help the gForce layout engine
				x: Math.random() * 500,
				y: Math.random() * 500
			};
		});

		this.graph.data({ nodes, edges: [] });
		this.graph.render();
		this.graph.fitView(40);
	}

}