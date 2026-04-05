import { BaseController } from "../../@still/component/super/service/BaseController.js";
import { PivotCreateComponent } from "../components/dataviz/bi/pivot/PivotCreateComponent.js";

export class PivotTableController extends BaseController {

    /** @type { PivotCreateComponent } */
    obj;

    static totalSavePivot = 0;
    static currentPivotId;

    allowDrop(e) { e.preventDefault(); e.currentTarget.classList.add('drag-over'); }
    
    handleDrop(e) {
        e.preventDefault(); this.clearDrag(e);
        const f = e.dataTransfer.getData("text"); const z = e.currentTarget.id;
        if (z === 'vals') { if (!this.obj.selection.vals.find(v => v.field === f)) this.obj.selection.vals.push({ field: f, mode: 'sum' }); }
        else { if (!this.obj.selection[z].includes(f)) this.obj.selection[z].push(f); }
        this.renderAll();
    }

    clearDrag(e) { e.currentTarget.classList.remove('drag-over'); }

    renderAll() {
        const container = this.obj.$parent.popup.querySelector('#table-canvas');
        const showAllRows = this.obj.container.querySelector('#show-all-rows-check').checked;
		const { selection, filters, parseEvents } = this.obj;

        ['rows', 'cols', 'vals'].forEach(id => {
            const z = this.obj.container.querySelector('#'+id);
            z.innerHTML = `<div class="zone-title">${id}</div>`;
            selection[id].forEach((item, idx) => {
                const chip = document.createElement('div'); chip.className = 'chip';
                const fName = id === 'vals' ? item.field : item;
                if (id === 'vals') {
                    chip.innerHTML = parseEvents(`${fName} <select class="agg-select" onchange="controller.onAggregationTypeChange(${idx}, this.value)">${this.obj.modes.map(m=>`<option value="${m}" ${item.mode===m?'selected':''}>${m.toUpperCase()}</option>`).join('')}</select>`);
                } else {
                    chip.innerHTML = parseEvents(`${fName} <span style="cursor:pointer;opacity:0.8;" onclick="controller.openFilter(event, '${fName}')">Y</span>`);
                }
                chip.innerHTML += parseEvents(`<span style="cursor:pointer;margin-left:5px;" onclick="controller.removeField('${id}','${fName}')">×</span>`);
                z.appendChild(chip);
            });
        });

        if (!selection.rows.length || !selection.vals.length) { 
            container.innerHTML = '<div class="empty-msg">Drag fields here to start.</div>'; 
            return; 
        }

        container.innerHTML = `
            <div class="lab-loader">
                <div class="spinner"></div>
                <div style="margin-left:10px; font-weight:bold; color:var(--primary);">Recalculating 50k rows...</div>
            </div>
            ${container.innerHTML} 
        `;

        requestAnimationFrame(() => {
            setTimeout(() => {
                const { root, cols } = this.buildTree(selection, filters, showAllRows);
                const heatmapCheck = this.obj.container.querySelector('#heatmap-check').checked;
                const htmlString = this.getTableHTML(root, cols, selection, heatmapCheck);
                this.updateTableDOM(container, htmlString); 
            }, 10);
        });
    }

    removeField(z, f) { this.obj.selection[z] = this.obj.selection[z].filter(x => (z==='vals'?x.field!==f:x!==f)); this.renderAll(); }

    openFilter(e, f) {
        e.stopPropagation();
        this.obj.activeFilterField = f;
        const modal = this.obj.container.querySelector('#filter-modal');
        const list = this.obj.container.querySelector('#modal-list');
        const rect = e.target.getBoundingClientRect();
        modal.style.display = 'block';
        modal.style.top = (rect.bottom + window.scrollY) + 'px';
        modal.style.left = rect.left + 'px';
        this.obj.container.querySelector('#modal-title').textContent = `Filter: ${f}`;
        const uniqueVals = [...new Set(this.obj.dataset.map(item => item[f]))];
        list.innerHTML = uniqueVals.map(v => `
            <label style="display:block; font-size:12px; margin-bottom:4px;">
                <input type="checkbox" ${filters[f].includes(v) ? 'checked' : ''} 
                    onchange="toggleFilterValue('${v}')"> ${v}
            </label>
        `).join('');
    }

    onAggregationTypeChange(idx, value){
        this.obj.selection.vals[idx].mode = value; this.renderAll()
    }

    updateTableDOM(container, htmlString) {
        const range = document.createRange();
        range.selectNode(container);
        const fragment = range.createContextualFragment(htmlString);
        
        container.innerHTML = '';
        container.appendChild(fragment);
    }

    buildTree(sel, fltrs, showAllRows = false) {

        const root = { children: {}, values: {}, label: 'Grand Total', depth: -1 };
        const allCols = new Set();
        
        const effectiveRows = showAllRows ? [...sel.rows, '__rowId'] : sel.rows;

        this.obj.dataset.forEach(item => {
            if ([...sel.rows, ...sel.cols].some(f => !fltrs[f]?.includes(item[f]))) return;
            
            if (this.searchQuery) {
                const matchFound = sel.rows.some(f => String(item[f]).toLowerCase().includes(this.searchQuery));
                if (!matchFound) return;
                let pathArr = [];
                sel.rows.forEach(f => {
                    pathArr.push(item[f]);
                    this.obj.expandedPaths.add(pathArr.join('|'));
                });
            }

            const cKey = sel.cols.length > 0 ? sel.cols.map(f => item[f]).join(' | ') : "Value";
            allCols.add(cKey);
            const update = (node, key) => {
                sel.vals.forEach(v => {
                    const k = `${key}_${v.field}`;
                    if (!node.values[k]) node.values[k] = { sum: 0, count: 0, max: -Infinity };
                    const calc = this.obj.calculatedFields.find(c => c.name === v.field);
                    const val = calc ? this.obj.evalFormula(item, calc.formula) : item[v.field];
                    node.values[k].sum += val; node.values[k].count += 1;
                    node.values[k].max = Math.max(node.values[k].max, val);
                });
            };
            update(root, cKey); update(root, 'TOTAL');
            let curr = root;
            effectiveRows.forEach((f, i) => {
                const val = item[f];
                if (!curr.children[val]) curr.children[val] = { children: {}, values: {}, depth: i };
                curr = curr.children[val]; update(curr, cKey); update(curr, 'TOTAL');
            });
        });
        return { root, cols: Array.from(allCols).sort() };
    }

    getTableHTML(root, cols, sel, heatmapOn) {
        const buffer = [];
        const stats = {};

        if (heatmapOn) {
            const scan = (n) => {
                cols.forEach(c => sel.vals.forEach(v => {
                    const k = `${c}_${v.field}`;
                    const val = this.getVal(n.values[k], v.mode);
                    if (!stats[k]) stats[k] = { min: Infinity, max: -Infinity };
                    stats[k].min = Math.min(stats[k].min, val); 
                    stats[k].max = Math.max(stats[k].max, val);
                }));
                Object.values(n.children).forEach(scan);
            };
            scan(root);
        }

        buffer.push('<table><thead><tr><th>Dimensions</th>');
        cols.forEach(c => sel.vals.forEach(v => {
            buffer.push(`<th>${c}<br><small>${v.field}</small></th>`);
        }));
        buffer.push('<th class="total-col">Total</th></tr></thead><tbody>');

        const rowTpl = (node, label, isGrand = false) => {
            let tr = `<tr class="${isGrand ? 'grand-total-row' : ''}">`;
            tr += `<td class="row-label-cell" style="padding-left: ${isGrand ? 12 : (node.depth * 25) + 12}px">`;
            
            if (!isGrand && Object.keys(node.children).length > 0) {
                tr += this.obj.parseEvents(`<span class="toggle-btn" onclick="controller.toggle('${label}')">${this.obj.expandedPaths.has(label) ? '−' : '+'}</span>`);
            }
            
            let cleanLabel = label.split('|').pop();
            tr += (isGrand ? 'Grand Total' : cleanLabel) + `</td>`;

            cols.forEach(c => sel.vals.forEach(v => {
                let k = `${c}_${v.field}`, style = "";
                const val = this.getVal(node.values[k], v.mode);
                if (heatmapOn && stats[k]?.max !== stats[k]?.min) {
                    const pct = (val - stats[k].min) / (stats[k].max - stats[k].min);
                    style = `style="background: rgba(37, 99, 235, ${pct * 0.35})"`;
                }
                tr += `<td ${style}>${Math.round(val).toLocaleString()}</td>`;
            }));

            sel.vals.forEach(v => {
                const tVal = this.getVal(node.values[`TOTAL_${v.field}`], v.mode);
                tr += `<td class="total-col">${Math.round(tVal).toLocaleString()}</td>`;
            });
            return tr + `</tr>`;
        };

        buffer.push(rowTpl(root, "Grand Total", true));
        
        const draw = (node, path = "") => {
            Object.keys(node.children).sort().forEach(k => {
                const p = path ? `${path}|${k}` : k;
                buffer.push(rowTpl(node.children[k], p));
                if (this.obj.expandedPaths.has(p)) draw(node.children[k], p);
            });
        };
        
        draw(root);
        buffer.push('</tbody></table>');
        return buffer.join('');
    }

    getVal(m, mode) {
        if (!m) return 0;
        if (mode === 'sum') return m.sum;
        if (mode === 'avg') return m.count ? m.sum / m.count : 0;
        if (mode === 'count') return m.count;
        if (mode === 'max') return m.max;
        return 0;
    }

    toggle(p) { 
        this.obj.expandedPaths.has(p) ? this.obj.expandedPaths.delete(p) : this.obj.expandedPaths.add(p); this.renderAll(); this.renderDashboard(); 
    }

    renderDashboard(container, pivotTile) {
        const pivot = this.renderPivotOnDashboard(pivotTile, PivotTableController.currentPivotId);
        container.appendChild(pivot.tile);
        pivot.runDataLoad();
    }

    renderPivotOnDashboard(cfg, i) {
        const tile = document.createElement('div'); 
        tile.className = 'dash-tile', tile.id = `pivotWrap_${Date.now()}`;
        tile.innerHTML = this.obj.parseEvents(`
            <div id="loader-${i}" class="loading-overlay">
                <div class="spinner"></div>
                <div style="font-size: 11px; font-weight: 600; color: #64748b;">Processing 50,000 Rows...</div>
            </div>
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:2px; flex-shrink:0;padding: 0 8px;">
                <h4 style="margin:0;">${cfg.name}</h4>
                <button onclick="controller.onDashboardPivotDelete('${cfg.id}','${tile.id}')" style="cursor:pointer;border:none;background:none;font-size:16px;font-size: 22px;margin-top: -10px;">&Cross;</button>
            </div>
            <div style="overflow:auto; flex:1; width:100%; border-top:1px solid #eee; padding-top:0px;" id="tile-${i}"></div>
        `);

        const { dataset, calculatedFields } = this.obj;
        return { 
            tile, 
            runDataLoad: () => this.obj.dashWorker.postMessage({ dataset, cfg, searchQuery: this.searchQuery, calculatedFields, tileIndex: i }) 
        };
    }

    onDashboardPivotDelete = (id, wrapId) =>
        this.obj.$parent.controller.removeFromDash(id, wrapId);

    handleDashDrop(e, container) {
        e.preventDefault(); e.currentTarget.classList.remove('drag-over');
        if (e.dataTransfer.getData("type") === "config") {
            const idx = e.dataTransfer.getData("pivotIndex");
            this.obj.$parent.controller.saveDashboardTile(this.obj.$parent.state.savedCharts[idx]);
            this.renderDashboard(container, this.obj.$parent.state.savedCharts[idx]);
            return idx;
        }
    }

    addCalculatedField() {
        const n = this.obj.container.querySelector('#calc-name').value; const f = this.obj.container.querySelector('#calc-formula').value;
        if (n && f) { this.obj.calculatedFields.push({ name: n, formula: f }); this.obj.container.querySelector('#calc-modal').style.display='none'; this.initSidebar(); }
    }

    initSidebar() {

		const { filters, dataset } = this.obj;
        const fieldList = this.obj.$parent.popup.querySelector('#source-fields');
        fieldList.innerHTML = '';
        
        [...this.obj.baseFields, ...this.obj.calculatedFields.map(cf => cf.name)].forEach(f => {
            const div = document.createElement('div');
            div.className = 'field-item' + (this.obj.calculatedFields.find(c => c.name === f) ? ' calc-field' : '');
            div.textContent = f; div.draggable = true; div.style.marginBottom = "8px";
            div.ondragstart = (e) => { 
                e.dataTransfer.setData("type", "field"); e.dataTransfer.setData("text", f); 
            };
            fieldList.appendChild(div);
            if (filters[f] === undefined) filters[f] = [...new Set(dataset.map(item => item[f]))];
        });

        const savedList = this.obj.$parent.popup.querySelector('.saved-pivots');
        savedList.innerHTML = '';

        Object.values(this.obj.$parent.state.savedCharts).forEach((cfg) => {
            if(cfg?.type == 'pivotTable'){
                const div = document.createElement('div');
                div.className = 'table-item active'; div.textContent = cfg.name; div.draggable = true;
                div.ondragstart = (e) => { 
                    PivotTableController.currentPivotId = 'pivot-'+cfg.id;
                    e.dataTransfer.setData("type", "config"), e.dataTransfer.setData("pivotIndex", 'pivot-'+cfg.id); 
                };
                savedList.appendChild(div);
            }
        });

        setTimeout(() => this.obj.$parent.controller.checkScroll(fieldList), 100);
    }

    searchTimer = null;
    searchQuery = "";

    handleSearch(v) {
        clearTimeout(this.searchTimer);
        this.searchTimer = setTimeout(() => {
            this.searchQuery = v.toLowerCase(); 
            if (!this.searchQuery) this.obj.expandedPaths.clear(); 
            this.renderAll();
        }, 250);
    }

    saveConfiguration() {
		const { selection, filters } = this.obj;
        const heatmap = this.obj.container.querySelector('#heatmap-check').checked;
        const showAllRows = this.obj.container.querySelector('#show-all-rows-check').checked;
        if (!selection.rows.length || !selection.vals.length) return alert("Empty Layout");
        const name = prompt("Name your layout:");
        if (name) {
            this.obj.$parent.state.savedCharts['pivot-'+PivotTableController.totalSavePivot] = {
                name, heatmap, showAllRows, selection: JSON.parse(JSON.stringify(selection)), filters: JSON.parse(JSON.stringify(filters)), 
                type: 'pivotTable', id: PivotTableController.totalSavePivot
            }
            PivotTableController.totalSavePivot++;
            this.initSidebar();
        }
    }

}