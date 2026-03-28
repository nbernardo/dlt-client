import { BaseController } from "../../@still/component/super/service/BaseController.js";
import { BIUserInterfaceComponent } from "../components/dataviz/bi/BIUserInterfaceComponent.js";
import { BiUiUtil } from "../components/dataviz/bi/util.js";

export class BIController extends BaseController {

    /** @type { BIUserInterfaceComponent } */
    obj;

    renderTableList() {
		const tables = this.obj.MOCK_TABLES[this.obj.state.pipeline] || [];
		this.obj.popup.querySelector("#tableList").innerHTML = tables
			.map((t) =>
				this.obj.parseEvents(
                    `<div class="table-item ${t.name === this.obj.state.activeTable ? "active" : ""}" onclick="controller.loadTable('${t.name}')">
                        <div class="table-icon">T</div>${t.name.replace(/_/g, " ")}
                        <span class="table-rows">${t.rows.toLocaleString()}</span>
                    </div>`
                )
			)
			.join("");
	}

    loadTable(name) {
        this.obj.state.activeTable = name;
        this.obj.state.filteredRows = [...this.obj.MOCK_DATA];
        this.obj.state.selectedRows.clear();
        this.renderTableList();
        this.renderSheet();
        this.populateAxisSelects();
    }

    renderSheet(){

        const rows = this.obj.state.filteredRows;
        
        if(!rows.length) return;
        const cols = Object.keys(rows[0]);
        
        const { state } = this.obj;

        this.obj.popup.querySelector('#tableHead').innerHTML=`<tr>
            <th class="row-num">#</th>
            ${cols.map(c => this.obj.parseEvents(`
                <th class="${state.sortCol === c ? 'sorted' : ''} ${state.frozenCols.has(c) ? 'frozen' : ''}">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <span onclick="controller.sortBy('${c}')" style="cursor:pointer; flex:1;">
                            ${c} <span>${state.sortCol === c ? (state.sortDir === 'asc' ? '↑' : '↓') : '↕'}</span>
                        </span>
                        <div class="th-actions">
                            <span class="freeze-btn ${state.frozenCols.has(c) ? 'frozen' : ''}" onclick="controller.toggleFreeze('${c}')" title="Freeze">❄</span>
                            <span onclick="removeColumn('${c}')" style="color:var(--danger); cursor:pointer; margin-left:8px; font-weight:bold; font-size:14px;" title="Delete Column">×</span>
                        </div>
                    </div>
                </th>`)).join('')}
        </tr>`;

        const cf = this.obj.popup.querySelector('#colFilter');

        cf.innerHTML = '<option value="">All columns</option>'+cols.map(c=>`<option value="${c}" ${cf.value === c ? 'selected' : ''}>${c}</option>`).join('');

        this.obj.popup.querySelector('#tableBody').innerHTML = rows.map((row,i) => 
            this.obj.parseEvents(
                `<tr class="${state.selectedRows.has(i) ? 'selected' : ''}" onclick="controller.toggleRow(${i})">
                    <td class="row-num">${i+1}</td>
                    ${cols.map(c=>`<td class="${BiUiUtil.cellClass(row[c])} ${state.frozenCols.has(c) ? 'frozen' : ''}" title="${row[c] ?? ''}">${BiUiUtil.formatCell(row[c])}</td>`).join('')}
                </tr>`
            )
        ).join('');

        this.obj.popup.querySelector('#rowCount').textContent = rows.length.toLocaleString();
        this.obj.popup.querySelector('#colCount').textContent = cols.length;
        this.obj.popup.querySelector('#selCount').textContent = state.selectedRows.size;
    }

    initInsertLogic() {
        const container = this.obj.popup.querySelector('.tableContainer');
        const line = this.obj.popup.querySelector('#colInsertLine');
        const { state } = this.obj;

        container.addEventListener("mousemove", (e) => {

            const ths = Array.from(this.obj.popup.querySelectorAll("#tableHead th"));
            if (ths.length < 2) return;

            const rect = container.getBoundingClientRect();
            const mouseX = e.clientX;
            let found = false;

            for (let i = 1; i < ths.length; i++) {
                const thRect = ths[i].getBoundingClientRect();

                if (Math.abs(mouseX - thRect.left) < 12) {
                    const scrollOffset = container.scrollLeft;
                    line.style.left = thRect.left - rect.left + scrollOffset + "px";
                    line.style.display = "block";
                    state.activeInsertIndex = i - 1;
                    found = true;
                    break;
                }
            }

            if (!found) {
                line.style.display = "none";
                state.activeInsertIndex = -1;
            }
        });
    }

    insertColumnAtGap() {

        const { state } = this.obj;

        if (state.activeInsertIndex === -1) return;
        const name = prompt("Enter new column name:");
        if (!name) return;

        // To preserve visual order in the object keys:
        this.obj.MOCK_DATA = this.obj.MOCK_DATA.map(row => {
            const keys = Object.keys(row);
            const newRow = {};
            keys.forEach((key, idx) => {
                newRow[key] = row[key];
                if (idx === state.activeInsertIndex) {
                    newRow[name] = "-";
                }
            });
            return newRow;
        });

        loadTable(state.activeTable);
        this.showToast(`Column "${name}" inserted.`);
    }

    toggleFreeze(col) {
        if (state.frozenCols.has(col)) state.frozenCols.delete(col);
        else state.frozenCols.add(col);
        renderSheet();
    }

    addColumn() {
        const name = prompt("Enter new column name:");
        if (!name) return;

        this.obj.MOCK_DATA.forEach((row) => (row[name] = "-"));

        this.loadTable(state.activeTable);
        
        this.showToast(`Column "${name}" added`);
    }

    filterRows(q) {
        
        const col = this.obj.popup.querySelectorAll('#colFilter').value;
        const lq = q.toLowerCase();

        this.obj.state.filteredRows = this.obj.MOCK_DATA.filter((row) => {
            if (!q) return true;
            if (col)
                return String(row[col] ?? "").toLowerCase().includes(lq);

            return Object.values(row).some((v) =>
                String(v ?? "").toLowerCase().includes(lq)
            );
        });
        this.obj.state.selectedRows.clear();
        this.renderSheet();
    }

    sortBy(col) {
        const { state } = this.obj;

        if (state.sortCol === col) state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
        else {
            state.sortCol = col;
            state.sortDir = "asc";
        }
        state.filteredRows.sort((a, b) => {
            const av = a[col],
                bv = b[col];
            if (av === bv) return 0;
            const cmp = av < bv ? -1 : 1;
            return state.sortDir === "asc" ? cmp : -cmp;
        });
        this.renderSheet();
    }

    toggleRow(i) {
        const { state } = this.obj;
        if (state.selectedRows.has(i)) state.selectedRows.delete(i);
        else state.selectedRows.add(i);
        document.getElementById("selCount").textContent = state.selectedRows.size;
        renderSheet();
    }

    exportCSV() {
        const rows = this.obj.state.filteredRows;
        if (!rows.length) return;

        const cols = Object.keys(rows[0]);
        const csv = [cols.join(","), ...rows.map((r) => cols.map((c) => `"${r[c] ?? ""}"`).join(","))].join("\n");

        const a = document.createElement("a");

        a.href = "data:text/csv," + encodeURIComponent(csv);
        a.download = this.obj.state.activeTable + ".csv";
        a.click();

        this.showToast("CSV exported");
    }

	switchTab(id, el) {
        
        if(id === 'sheet') this.obj.init();

		this.obj.popup.querySelectorAll(".tab").forEach((t) => t.classList.remove('active'));
		el.classList.add("active");
		this.obj.popup.querySelectorAll(".content").forEach((c) => c.classList.remove('active'));
		this.obj.popup.querySelector(`.tab-${id}`).classList.add('active');

	}

    showToast = (msg) => BiUiUtil.showToast(this.obj.popup.querySelector('#toast'), msg);

    renderChartTypeGrid() {
        const { state, CHART_TYPES, parseEvents } = this.obj;
        
        this.obj.popup.querySelector('.chartTypeGrid').innerHTML = CHART_TYPES.map(
            (t) => parseEvents(
                `<div class="chart-type-btn ${t.id === state.chartType ? "active" : ""}" onclick="controller.selectChartType('${t.id}')"><span class="chart-type-icon">${t.icon}</span>${t.label}</div>`
            )
        ).join("");
    }

    selectChartType(id){
        this.obj.state.chartType = id; 
        this.renderChartTypeGrid();
    }

    renderColorRow() {
        const { state, parseEvents } = this.obj;
        document.getElementById("colorRow").innerHTML = BiUiUtil.chartColors.map(
            (c) => parseEvents(
                `<div class="color-swatch ${c === state.chartColor ? "active" : ""}" style="background:${c}" onclick="controller.selectColor('${c}')"></div>`
            )
        ).join("");
    }

    selectColor(c){
        this.obj.state.chartColor = c; this.renderColorRow();
    }

    populateAxisSelects() {

        const { MOCK_DATA } = this.obj;
        const cols = MOCK_DATA.length ? Object.keys(MOCK_DATA[0]) : [];
        const opts = cols.map((c) => `<option value="${c}">${c}</option>`).join("");

        this.obj.popup.querySelector('#xAxisSelect').innerHTML = opts;
        this.obj.popup.querySelector('#yAxisSelect').innerHTML = opts;

        if (cols.length > 1) this.obj.popup.querySelector('#yAxisSelect').value = cols[1];

    }

    buildChart() {

        const { state, CHART_TYPES } = this.obj;
        const xCol = this.obj.popup.querySelector('#xAxisSelect').value;
        const yCol = this.obj.popup.querySelector('#yAxisSelect').value;
        const agg = this.obj.popup.querySelector('#aggSelect').value;

        const title = this.obj.popup.querySelector('.chartTitleInput').value || 'Chart';

        const ctDef = CHART_TYPES.find(t => t.id === state.chartType);
        
        this.obj.popup.querySelector('#previewTitle').textContent = title;
        
        let labels, values;
        if (agg === 'none') {
            const slice = state.filteredRows.slice(0, 50);
            labels = slice.map(r => String(r[xCol] ?? ''));
            values = slice.map(r => Number(r[yCol]) || 0);
        } else {
            const grouped = {};
            state.filteredRows.forEach(r => {
                const k = String(r[xCol] ?? 'null');
                if (!grouped[k]) grouped[k] = [];
                grouped[k].push(Number(r[yCol]) || 0);
            });
            labels = Object.keys(grouped);
            values = labels.map(k => {
                const vals = grouped[k];
                if (agg === 'sum') return vals.reduce((a, b) => a + b, 0);
                if (agg === 'avg') return vals.reduce((a, b) => a + b, 0) / vals.length;
                if (agg === 'max') return Math.max(...vals);
                if (agg === 'min') return Math.min(...vals);
                return vals.length; // count
            });
        }

        if (state.chartInstance) state.chartInstance.destroy();
        const ctx = this.obj.popup.querySelector('#chartCanvas').getContext('2d');
        
        state.chartInstance = new Chart(ctx, {
            type: ctDef.cjsType,
            data: {
                labels,
                datasets: [{
                    label: yCol,
                    data: values,
                    backgroundColor: state.chartColor + 'cc',
                    borderColor: state.chartColor,
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: state.chartType === 'horizontalBar' ? 'y' : 'x',
                plugins: { legend: { display: ['pie', 'doughnut'].includes(state.chartType) } }
            }
        });

        state.pendingChart = { 
            id: Date.now(), 
            title, 
            type: state.chartType, 
            config: { labels, values, color: state.chartColor, yLabel: yCol, cjsType: ctDef.cjsType } 
        };
    }

    saveChart() {
        if (!this.obj.state.pendingChart) return showToast("Build a chart first!");
        this.obj.state.savedCharts.push({...this.obj.state.pendingChart});
        this.renderSavedCharts();
        this.showToast("Chart saved to library");
    }

    renderSavedCharts() {

        const list = this.obj.popup.querySelector('.chartList');

        if (!this.obj.state.savedCharts.length) {
            list.innerHTML = '<div style="padding:10px; color:var(--muted2); font-size:11px;">No saved charts</div>';
            return;
        }

        list.innerHTML = this.obj.state.savedCharts.map((c, i) => this.obj.parseEvents(`
            <div class="chart-thumb" draggable="true" onclick="controller.loadSavedChart(${c.id})" ondragstart="controller.handleDragStart(event, ${i})">
                ${c.title} <span class="chart-type-badge">${c.type}</span>
            </div>`)
        ).join('');
    }

	handleDragStart = (e, index) => e.dataTransfer.setData('chartIdx', index);

    initDragAndDrop() {
        const { state } = this.obj;
        const grid = this.obj.popup.querySelector('.dashGrid');
        const self = this;
        
        grid.addEventListener('dragover', e => { 
            e.preventDefault(); 
            grid.classList.add('drag-over'); 
        });
        
        grid.addEventListener('dragleave', () => { 
            grid.classList.remove('drag-over'); 
        });
        
        grid.addEventListener('drop', e => {
            e.preventDefault();
            grid.classList.remove('drag-over');
            
            const idx = e.dataTransfer.getData('chartIdx');
            if (idx !== "") {
                const chartData = state.savedCharts[parseInt(idx)];
                if (chartData) {
                    if (!state.dashboards[state.activeDash]) state.dashboards[state.activeDash] = [];
                    state.dashboards[state.activeDash].push({...chartData, instanceId: Date.now()});
                    this.loadDashboard(state.activeDash);
                    this.showToast(`Added ${chartData.title} to dashboard`);
                }
            }
        });
    }

    loadDashboard(name) {
        this.obj.state.activeDash = name;

        this.obj.popup.querySelector('.dashSelect').value = name;
        const grid = this.obj.popup.querySelector('.dashGrid');

        const items = this.obj.state.dashboards[name] || [];
        
        if (items.length === 0) {
            grid.innerHTML = `<div class="empty-dashboard">
                <div class="empty-icon">📊</div>
                <div>Drag charts from the sidebar to populate this dashboard</div>
            </div>`;
            return;
        }

        grid.innerHTML = items.map((c, i) => this.obj.parseEvents(`
            <div class="dashboard-card">
                <div class="dashboard-card-header">
                    <div class="dashboard-card-title">${c.title}</div>
                    <button class="icon-btn" onclick="controller.removeFromDash(${i})">×</button>
                </div>
                <div class="dashboard-card-body">
                    <canvas id="dashCanvas-${i}" class="dashboard-card-canvas"></canvas>
                </div>
            </div>`)
        ).join('');

        // Initialize small charts in cards
        items.forEach((c, i) => {
            const ctx = document.getElementById(`dashCanvas-${i}`).getContext('2d');
            new Chart(ctx, {
                type: c.config.cjsType,
                data: {
                    labels: c.config.labels,
                    datasets: [{
                        data: c.config.values,
                        backgroundColor: c.config.color + 'cc',
                        borderColor: c.config.color,
                        borderWidth: 1
                    }]
                },
                options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } }
            });
        });
    }

	removeFromDash(index) {
		this.obj.state.dashboards[this.obj.state.activeDash].splice(index, 1);
		this.loadDashboard(this.obj.state.activeDash);
	}

    renderDashboardSelect() {
        const names = Object.keys(this.obj.state.dashboards);
        const html = names.map(n => `<option value="${n}">${n}</option>`).join('');
        this.obj.popup.querySelector('.dashSelect').innerHTML = html;
        this.obj.popup.querySelector('.publishDashSelect').innerHTML = html;
    }

    openPublishModal() { this.obj.popup.querySelector('#publishModal').classList.add('open'); }
    closePublishModal() { this.obj.popup.querySelector('#publishModal').classList.remove('open'); }

    publishChart() {
        const { state } = this.obj;

        if (!state.pendingChart) return showToast("No chart to publish");
        let dashName = this.obj.popup.querySelector('#newDashInput').value || this.obj.popup.querySelector('#publishDashSelect').value;

        if (!state.dashboards[dashName]) state.dashboards[dashName] = [];

        state.dashboards[dashName].push({...state.pendingChart, instanceId: Date.now()});
        this.renderDashboardSelect();
        this.closePublishModal();
        this.showToast(`Published to ${dashName}`);
    }

    newDashboard() {
        const name = prompt("Dashboard Name:");
        if (name) {
            this.obj.state.dashboards[name] = [];
            this.renderDashboardSelect();
            this.loadDashboard(name);
        }
    }

	onPipelineChange(val) {
		this.obj.state.pipeline = val;
		const tables = this.obj.MOCK_TABLES[val] || [];
		this.obj.state.activeTable = tables[0]?.name || "";
		this.renderTableList();
		this.loadTable(this.obj.state.activeTable);
	}

    loadSavedChart(id) {

        const { state } = this.obj;

        const c = state.savedCharts.find((x) => x.id === id);
        if (!c) return;

        this.switchTab("chart", document.querySelectorAll(".tab")[1]);
        this.obj.popup.querySelector('.chartTitleInput').value = c.title;
        state.chartType = c.type;
        state.chartColor = c.config.color;
        this.renderChartTypeGrid();
        this.renderColorRow();
        state.pendingChart = c;
        this.buildChart();

    }
    
}