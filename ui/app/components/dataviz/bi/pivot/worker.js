self.onmessage = function(e) {
    
    const { dataset, cfg, searchQuery, calculatedFields, tileIndex } = e.data;
    const { selection: sel, filters: fltrs, showAllRows } = cfg;
    
    const root = { children: {}, values: {}, label: 'Grand Total', depth: -1 };
    const allCols = new Set();
    const effectiveRows = showAllRows ? [...sel.rows, '__rowId'] : sel.rows;

    const filterSets = {};
    for (let f in fltrs) filterSets[f] = new Set(fltrs[f]);

    const activeCalcs = calculatedFields.filter(cf => 
        sel.vals.some(v => v.field === cf.name)
    ).map(cf => ({
        ...cf,
        regex: new RegExp(cf.name, 'g'),
        dependencies: Object.keys(dataset[0] || {}).filter(k => cf.formula.includes(k))
    }));

    dataset.forEach(item => {
        for (let f in filterSets) {
            if (!filterSets[f].has(item[f])) return;
        }
        
        if (searchQuery && !sel.rows.some(f => String(item[f]).toLowerCase().includes(searchQuery))) return;

        const cKey = sel.cols.length > 0 ? sel.cols.map(f => item[f]).join(' | ') : "Value";
        allCols.add(cKey);

        const update = (node, key) => {
            sel.vals.forEach(v => {
                const k = `${key}_${v.field}`;
                if (!node.values[k]) node.values[k] = { sum: 0, count: 0, max: -Infinity };
                
                let val = item[v.field];                
                const calc = activeCalcs.find(c => c.name === v.field);
                if (calc) {
                    let f = calc.formula;
                    calc.dependencies.forEach(prop => {
                        f = f.replace(new RegExp(prop, 'g'), item[prop] || 0);
                    });
                    try { val = eval(f); } catch { val = 0; }
                }

                const nVal = Number(val) || 0;
                node.values[k].sum += nVal;
                node.values[k].count += 1;
                node.values[k].max = Math.max(node.values[k].max, nVal);
            });
        };

        update(root, cKey);  
        update(root, 'TOTAL');
        
        let curr = root;
        effectiveRows.forEach((f, i) => {
            const rowVal = item[f];
            if (!curr.children[rowVal]) curr.children[rowVal] = { children: {}, values: {}, depth: i };
            curr = curr.children[rowVal]; 
            update(curr, cKey); 
            update(curr, 'TOTAL');
        });
    });

    self.postMessage({ root, cols: Array.from(allCols).sort(), tileIndex, cfg });
};