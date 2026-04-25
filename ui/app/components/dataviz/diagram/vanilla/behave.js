/** CUSTOM NODE REGISTRATION */
G6.registerNode('db-table', {
  draw(cfg, group) {
    const { name, columns, collapsed, color = '#5B8FF9' } = cfg;
    const width = 200;
    const headerHeight = 35;
    const rowHeight = 25;
    const height = collapsed ? headerHeight : headerHeight + (columns.length * rowHeight);

    const keyShape = group.addShape('rect', {
      attrs: {
        x: 0, y: 0, width, height,
        fill: '#ffffff', stroke: color,
        lineWidth: 1, radius: 6,
        cursor: 'move',
      },
      name: 'table-container', draggable: true,
    });

    group.addShape('rect', {
      attrs: {
        x: 0, y: 0, width, height: headerHeight,
        fill: color, radius: [6, 6, 0, 0],
        cursor: 'move',
      },
      name: 'header', draggable: true,
    });

    group.addShape('text', {
      attrs: {
        x: width / 2, y: headerHeight / 2,
        textAlign: 'center', textBaseline: 'middle',
        text: name, fill: '#ffffff',
        fontWeight: 'bold', fontSize: 12, cursor: 'move',
      },
      name: 'header-text', draggable: true,
    });

    if (!collapsed) {
      columns.forEach((col, i) => {
        const yOffset = headerHeight + (i * rowHeight);
        
        group.addShape('rect', {
          attrs: {  x: 0, y: yOffset, width, height: rowHeight,  fill: '#fff',  cursor: 'move'  },
          name: `row-${col.name}`, draggable: true
        });

        group.addShape('text', {
          attrs: { x: 10, y: yOffset + rowHeight / 2, textBaseline: 'middle', text: col.name, fill: '#333', fontSize: 11, cursor: 'move' },
          name: `text-${col.name}`, draggable: true
        });
      });
    }

    return keyShape;
  },
  getAnchorPoints() { return [[0, 0.5], [1, 0.5]]; }
}, 'rect');

/** INITIALIZE GRAPH */
const graph = new G6.Graph({
  container: 'mountNode', width: window.innerWidth, height: window.innerHeight,
  modes: { default: ['drag-canvas', 'zoom-canvas', 'drag-node'], },
  defaultEdge: {
    type: 'polyline', lineAppendWidth: 10,
    style: { stroke: '#A3B1BF', lineWidth: 2, endArrow: true, radius: 10 }
  }
});

/** DATA */
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

/** EVENTS */
const toggleFieldHighlight = (edgeItem, isHovering) => {
  const model = edgeItem.getModel();
  const sourceNode = graph.findById(model.source);
  const targetNode = graph.findById(model.target);

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

  graph.updateItem(edgeItem, {
    style: { stroke: isHovering ? '#1890ff' : '#A3B1BF', lineWidth: isHovering ? 3 : 2 }
  });
};

graph.on('edge:mouseenter', (evt) => toggleFieldHighlight(evt.item, true));
graph.on('edge:mouseleave', (evt) => toggleFieldHighlight(evt.item, false));

graph.data(data);
graph.render();