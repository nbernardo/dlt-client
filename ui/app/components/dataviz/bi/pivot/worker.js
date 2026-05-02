self.onmessage = function(e) {
    const { 
        dataset = [], 
        cfg = {}, 
        searchQuery = "", 
        calculatedFields = [], 
        tileIndex, 
        isFetchFromDB = false, 
        globalFilters = {} 
    } = e.data;

    const sel = cfg.selection || { rows: [], cols: [], vals: [] };
    const showAllRows = cfg.showAllRows || false;
    const filters = cfg.filters || {}; 

    const root = { children: {}, values: {}, label: 'Grand Total', depth: -1 };
    const allCols = new Set();
    const effectiveRows = showAllRows ? [...sel.rows, '__rowId'] : sel.rows;
    const len = dataset.length;

    const filterSets = {};
    if (isFetchFromDB) {
        const filterArray = Array.isArray(filters) ? filters : Object.keys(filters);
        filterArray.forEach(fObj => {
            const key = (typeof fObj === 'string') ? fObj : Object.keys(fObj)[0];
            filterSets[key] = new Set();
        });
    } else {
        for (let f in filters) {
            // Only create a filter set if there are active selections in the UI
            if (Array.isArray(filters[f]) && filters[f].length > 0) {
                filterSets[f] = new Set(filters[f].map(v => String(v)));
            }
        }
    }

    const globalFilterKeys = globalFilters ? Object.keys(globalFilters).filter(k => globalFilters[k] && globalFilters[k].length > 0) : [];
    const hasGlobalFilters = globalFilterKeys.length > 0;

    const dataKeys = dataset.length > 0 ? Object.keys(dataset[0]) : [];
    let updateFnBody = "let v; const vals = node.values;";
    
    sel.vals.forEach(valObj => {
        const field = valObj.field;
        const calc = calculatedFields.find(cf => cf.name === field);
        const baseKey = `key + "_${field}"`;
        updateFnBody += `if(!vals[${baseKey}]) vals[${baseKey}] = {sum:0, count:0, max:-Infinity}; const entry_${field} = vals[${baseKey}];`;
        if (calc) {
            let f = calc.formula;
            dataKeys.forEach(dk => f = f.replace(new RegExp(`\\b${dk}\\b`, 'g'), `(Number(item["${dk}"]) || 0)`));
            updateFnBody += `try { v = Number(${f}) || 0; } catch { v = 0; }`;
        } else {
            updateFnBody += `v = (item["${field}"] === null || item["${field}"] === undefined) ? 0 : Number(item["${field}"]) || 0;`;
        }
        updateFnBody += `entry_${field}.sum += v; entry_${field}.count += 1; if(v > entry_${field}.max) entry_${field}.max = v;`;
    });

    const runUpdate = new Function('item', 'node', 'key', updateFnBody);
    const searchQ = searchQuery ? searchQuery.toLowerCase() : null;
    const reportChunk = 10000;

    for (let i = 0; i < len; i++) {
        const item = dataset[i];
        if (i > 0 && i % reportChunk === 0) 
            self.postMessage({ type: 'PROGRESS', progress: Math.round((i / len) * 100), tileIndex });

        if (hasGlobalFilters) {
            let failGlobal = false;
            for (let k = 0; k < globalFilterKeys.length; k++) {
                const f = globalFilterKeys[k];
                const val = (item[f] === null || item[f] === undefined) ? item[f] : String(item[f]);
                if (!globalFilters[f].includes(val)) {
                    failGlobal = true;
                    break;
                }
            }
            if (failGlobal) continue;
        }

        if (!isFetchFromDB) {
            let skip = false;
            for (let f in filterSets) {
                if(!(f in item)) continue;
                const val = (item[f] === null || item[f] === undefined) ? item[f] : String(item[f]);
                // If the value is not in the allowed set, skip the row
                if (!filterSets[f].has(val)) { skip = true; break; }
            }
            if (skip) continue;
        } else {
            for (let f in filterSets) {
                if(!(f in item)) continue;
                const val = (item[f] === null || item[f] === undefined) ? item[f] : String(item[f]);
                filterSets[f].add(val);
            }
        }
        
        if (searchQ) {
            let found = false;
            for (let r = 0; r < sel.rows.length; r++) {
                if (String(item[sel.rows[r]] || "").toLowerCase().includes(searchQ)) { 
                    found = true; 
                    break; 
                }
            }
            if (!found) continue;
        }

        const cKey = sel.cols.length > 0 ? sel.cols.map(f => {
            const v = item[f];
            return (v === null || v === undefined) ? "N/A" : v;
        }).join(' | ') : "Value";
        
        allCols.add(cKey);

        runUpdate(item, root, cKey); 
        runUpdate(item, root, 'TOTAL');
        
        let curr = root;
        for (let j = 0; j < effectiveRows.length; j++) {
            const rawVal = item[effectiveRows[j]];
            const rowVal = (rawVal === null || rawVal === undefined) ? "N/A" : rawVal;

            if (!curr.children[rowVal]) 
                curr.children[rowVal] = { children: {}, values: {}, depth: j };
            
            curr = curr.children[rowVal]; 
            runUpdate(item, curr, cKey); 
            runUpdate(item, curr, 'TOTAL');
        }
    }

    if (isFetchFromDB) {
        const uiFilters = {};
        for (let f in filterSets) uiFilters[f] = Array.from(filterSets[f]);
        cfg.filters = uiFilters;
    }
    
    self.postMessage({ type: 'RESULT', root, cols: Array.from(allCols).sort(), tileIndex, cfg });
};