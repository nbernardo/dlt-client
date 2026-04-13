import { sleepForSec } from "../../@still/component/manager/timer.js";
import { BaseController } from "../../@still/component/super/service/BaseController.js";
import { AppTemplate } from "../../config/app-template.js";
import { BIUserInterfaceComponent } from "../components/dataviz/bi/main/BIUserInterfaceComponent.js";
import { BiUiUtil } from "../components/dataviz/bi/util.js";
import { BIService } from "../services/BIService.js";
import { AIUtil } from "../util/AIUtil.js";

export class BIController extends BaseController {

    /** @type { BIUserInterfaceComponent } */
    obj;

    static instance = null;

    wasUiPreviousInited = false;

    constructor(){
        super();
        BIController.instance = this;
    }

    /** @returns { BIController } */
    static getObj = () => BIController.instance;

    renderTableList() {
		const tables = BIController.currentTableList || [];
		this.obj.popup.querySelector(".tableList").innerHTML = tables
			.map((t) =>
				this.obj.parseEvents(
                    `<div class="table-item active">
                        <div class="table-icon"><input type="checkbox" onclick="controller.loadTable('${t.name}',true, this.checked)"></div>${t?.name?.replace(/_/g, " ")}
                        <span class="table-rows">${t?.totalCols}</span>
                    </div>`
                )
			)
			.join("");
	}

    viewingTables = new Set();
    async loadTable(name, runAnalytics) {
        this.obj.state.activeTable = name;
        this.obj.state.filteredRows = [...this.obj.gridDataSource];
        this.obj.state.selectedRows.clear();
        this.populateAxisSelects();
        
        if(runAnalytics){
            if(this.viewingTables.has(name)) this.viewingTables.delete(name);
            else this.viewingTables.add(name);

            let colsDetails, columns = [];
            this.obj.popup.querySelector('#tableBody').innerHTML = `<tr><td>${this.dataProcessLoading()}</td></tr>`;
            for(const table of [...this.viewingTables]){
                colsDetails = (BIController.currentTableList.filter(tbl => tbl.name == table)[0]?.cols || []);
                columns.push(...colsDetails.map(itm => `${table}_${itm.column_name}`));
            }

            await this.runAnaluticsAndRenderSheet(columns.join(','))
        }
        
    }

    async runAnaluticsAndRenderSheet(fields, pipeline){
        const result = await this.sendAnalyticsRequest(fields, pipeline);
        await this.obj.setData((result.result || [])).init();
        this.renderSheet();
        return result.result;
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
                            <span onclick="controller.removeColumn('${c}')" style="color:var(--danger); cursor:pointer; margin-left:8px; font-weight:bold; font-size:14px;" title="Delete Column">×</span>
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
        this.obj.gridDataSource = this.obj.gridDataSource.map(row => {
            const keys = Object.keys(row);
            const newRow = {};
            keys.forEach((key, idx) => {
                newRow[key] = row[key];
                if (idx === (state.activeInsertIndex - 1)) {
                    newRow[name] = "-";
                }
            });
            return newRow;
        });

        this.loadTable(state.activeTable);
        this.showToast(`Column "${name}" inserted.`);
    }

    toggleFreeze(col) {
        const { state } = this.obj;
        if (state.frozenCols.has(col)) state.frozenCols.delete(col);
        else state.frozenCols.add(col);
        this.renderSheet();
    }

    addColumn() {
        const name = prompt("Enter new column name:");
        if (!name) return;

        this.obj.gridDataSource.forEach((row) => (row[name] = "-"));

        this.loadTable(this.obj.state.activeTable);
        
        this.showToast(`Column "${name}" added`);
    }

    filterRows(q) {
        
        const col = this.obj.popup.querySelectorAll('#colFilter').value;
        const lq = q.toLowerCase();

        this.obj.state.filteredRows = this.obj.gridDataSource.filter((row) => {
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

	async switchTab(id, el) {
        
        if(id === 'sheet') await this.obj.init();
        if(id === 'dashboard') this.obj.showDashboardActions = true;
        else {
            this.obj.showDashboardActions = false;
            this.obj.showDashboardActions = false;
        }

		this.obj.popup.querySelectorAll(".tab").forEach((t) => t.classList.remove('active'));
		el.classList.add("active");
		this.obj.popup.querySelectorAll(".content").forEach((c) => c.classList.remove('active'));
		this.obj.popup.querySelector(`.tab-${id}`).classList.add('active');

        if(id == 'chart') return;

        if(id == 'pivot'){
            this.obj.showTablesList = false;
            this.obj.showTablesList = false;
        }
        else {
            this.obj.showTablesList = true;
            this.obj.showTablesList = true;
        }

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

        const { gridDataSource } = this.obj;
        const cols = gridDataSource.length ? Object.keys(gridDataSource[0]) : [];
        const opts = cols.map((c) => `<option value="${c}">${c}</option>`).join("");

        this.obj.popup.querySelector('#xAxisSelect').innerHTML = opts;
        this.obj.popup.querySelector('#yAxisSelect').innerHTML = opts;

        if (cols.length > 1) this.obj.popup.querySelector('#yAxisSelect').value = cols[1];

    }

    buildChart(chart) {

        const { state, CHART_TYPES } = this.obj;
        const xCol = this.obj.popup.querySelector('#xAxisSelect').value;
        const yCol = this.obj.popup.querySelector('#yAxisSelect').value;
        const agg = this.obj.popup.querySelector('#aggSelect').value;

        const title = this.obj.popup.querySelector('.chartTitleInput').value || 'Chart';

        const ctDef = CHART_TYPES.find(t => t.id === state.chartType);
        
        this.obj.popup.querySelector('#previewTitle').textContent = title;
        
        let labels, values = [];
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
                    backgroundColor: values.map((_, i) => `hsla(${(i * 137.5) % 360}, 70%, 55%, 0.8)`),
                    borderColor: values.map((_, i) => `hsl(${(i * 137.5) % 360}, 70%, 45%, 1)`),
                    //backgroundColor: state.chartColor + 'cc',
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

        // Reassign the values in the chart which config are fetch form Database
        if(chart) chart.config.values = values;

        state.pendingChart = { 
            id: String(Math.random()).slice(2) + Date.now() + String(Math.random()).slice(2) + 'n', 
            title, 
            type: state.chartType, 
            config: { labels, values, color: state.chartColor, yLabel: yCol, cjsType: ctDef.cjsType },
            viewingTables: [...this.viewingTables],
            dataSource: this.obj.state.pipeline,
            xCol, 
            yCol,
            agg,
        };
    }

    saveChart() {
        if (!this.obj.state.pendingChart) return showToast("Build a chart first!");
        if(this.saveChartConfig()){
            this.obj.state.savedCharts['chart-'+this.obj.state.pendingChart.id] = { ...this.obj.state.pendingChart, fromDB: true };

            this.renderSavedCharts();
            this.showToast("Chart saved to library");
            AppTemplate.toast.success('Chart saved to library');
        }else{
            AppTemplate.toast.error('Error while saving Chart')
            this.showToast("Error while saving Chart");
        }
    }
        
    renderSavedCharts() {
        const list = this.obj.popup.querySelector('.chartList');

        if (!Object.keys(this.obj.state.savedCharts).length)
            return list.innerHTML = '<div style="padding:10px; color:var(--muted2); font-size:11px;">No saved charts</div>';

        const saveCharts = this.obj.state.savedCharts;       
        list.innerHTML = Object.values(saveCharts).filter(c => c.type != 'pivotTable').map((c, i) => this.obj.parseEvents(`
            <div class="chart-thumb" draggable="true" onclick="controller.loadSavedChart(${String(c.id)})" ondragstart="controller.handleDragStart(event, 'chart-${c.id}')">
                ${c.title} <span class="chart-type-badge">${c.type}</span>
            </div>`)
        ).join('');
    }

    static currentChartId = null;
	handleDragStart = (e, index) => {
        e.dataTransfer.setData('chartIdx', index);
        BIController.currentChartId = index;
    }

    isDraggingDashboardObject = false;
    async initDragAndDrop() {
        if(this.wasUiPreviousInited === false){
            this.wasUiPreviousInited = true;
            const grid = this.obj.popup.querySelector('.dashGrid');
            
            grid.addEventListener('dragover', e => { 
                e.preventDefault();  grid.classList.add('drag-over'); 
                this.isDraggingDashboardObject = true;
            });
            
            grid.addEventListener('dragleave', () => grid.classList.remove('drag-over'));
            
            grid.addEventListener('drop', e => {
                e.preventDefault();
                
                grid.classList.remove('drag-over');
                const pvtIndex = e.dataTransfer.getData("pivotIndex");
                const idx = e.dataTransfer.getData('chartIdx');

                if (idx !== '' || pvtIndex !== '') {                    
                    const chartData = this.obj.state.savedCharts[(idx || pvtIndex)];                    
                    if (chartData) {
                        this.saveDashboardTile(chartData);
                        this.loadDashboard(this.obj.state.activeDash, pvtIndex, e);
                        this.showToast(`Added ${chartData.title} to dashboard`);
                    }
                }
            });
        }
    }

    saveDashboardTile(chartData){
        const { state } = this.obj; 
        if(chartData.saved !== true){
            if (!state.dashboards[state.activeDash]) state.dashboards[state.activeDash] = [];
            if(chartData.selection) chartData.selection.datasource = this.obj.state.pipeline;
            state.dashboards[state.activeDash].push({...chartData, instanceId: Date.now()});
            chartData.saved = true;
        }
    }

    /** @returns { HTMLElement } */
    static getDashboardGrid = () => BIController.get().obj.popup.querySelector('.dashGrid');


    static dashboardAddedCharts = new Set();
    async loadDashboard(name, isPivot, event, isDashboardChange = false) {

        if(this.obj.state.activeDash == name && !this.isDraggingDashboardObject) return;
        if(!this.obj.state.chartsByDashboard[name]) this.obj.state.chartsByDashboard[name] = new Set();
        
        this.obj.state.activeDash = name;
        let items, grid = this.obj.popup.querySelector('.dashGrid');
        
        grid.classList.remove('empty-dashboard');
        grid.querySelectorAll('.dash-empty').forEach(el => el.remove());

        const { charts, dataSources, importedDash } = this.extractDashboardDetailes(name);

        if ((charts || []).length === 0) {
            grid.classList.add('empty-dashboard');
            return grid.innerHTML = `<div class="empty-icon dash-empty">📊</div><div class="dash-empty">Drag charts from the sidebar to populate this dashboard</div>`;
        }

        // This will extract the charts/pivot tables. 
        // And in case it's needed, it'll fetch the data from the Backend
        items = await this.extractChartsAndData(grid, charts, name, dataSources, isDashboardChange, importedDash);

        if(isPivot)
            return this.addPivotToDashboard(event, grid, name);
    
        if(items.length > 1){
            for(const itm of items) this.addChartToDashBoard(grid, itm, name, event);
        } else 
            this.addChartToDashBoard(grid, items[0], name, event);
        
        items.forEach((c, i) => {

            if(c.type == 'pivotTable' || c.id?.startsWith('pivot-')) return;

            if(importedDash){
                const existingChart = Chart.getChart(`dashCanvas-${c.id}`);
                if(existingChart) existingChart.destroy();
            }

            const ctx = document.getElementById(`dashCanvas-${c.id}`).getContext('2d');
            new Chart(ctx, {
                type: c.config.cjsType,
                data: {
                    labels: c.config.labels,
                    datasets: [{ data: c.config.values, backgroundColor: c.config.color + 'cc', borderColor: c.config.color, borderWidth: 1 }]
                },
                options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } }
            });
            this.obj.state.chartsByDashboard[name].add(c.id);
        });
        this.isDraggingDashboardObject = false;       
    }

    addPivotToDashboard(event, grid, name, chart, isFetchFromDB){
        const pivotId = this.obj.pivotTableProxy.controller.handleDashDrop(event, grid, chart, isFetchFromDB);        
        this.obj.state.chartsByDashboard[name].add(pivotId);
        return BIController.dashboardAddedCharts.add(pivotId);
    }

    async extractChartsAndData(grid, charts, name, dataSources, isDashboardChange, importedDash){

        const isChartAdded = (id) => this.obj.state.chartsByDashboard[name].has(id);

        if(isDashboardChange){
            
            grid.innerHTML = '';
            if(importedDash){
                
                const fields = this.genDuckDBFieldNames(dataSources.tables);
                let data = await this.runAnaluticsAndRenderSheet(fields, dataSources.datasource);
                const dataPointerID = BIService.setDashboardDataPointer(data);
                BIService.assigneDataSourcePerTable(dataSources.tables, name, dataPointerID);

                return (charts || []).map(chart => {
                    chart.dataPointer = dataPointerID;
                    if(chart.config)
                        chart.config.values = data.map(fields => fields[chart?.config?.yLabel]);
                    return chart;
                });
            }else
                return (charts || []);

        }else{
            return (charts || []).filter(c => !isChartAdded(c?.id));
        }

    }


    extractDashboardDetailes(name){

        const dashboard = this.obj.state.dashboards[name];
        
        let [importedDash, dataSources, charts] = [false, {}, dashboard];
        if((dashboard || []).length){
            if(dashboard[0] == 'imported'){
                [importedDash, dataSources, charts] = [true, dashboard[1], dashboard.slice(2)];
                this.obj.state.dashboards[name] = dashboard.slice(2);
                return { importedDash, dataSources, charts }
            }
        }
        return { importedDash, dataSources, charts };
    }

    addChartToDashBoard(grid, chart, name, event){
        
        if(chart.type === 'pivotTable' || String(chart.id).startsWith('pivot-')){
            const isFetchFromDB = 'dataPointer' in chart;
            return this.addPivotToDashboard(event, grid, name, chart, isFetchFromDB);
        }
        
        const chartContent = (title, cId, wrapperId) => `
            <div class="dashboard-card">
                <div class="dashboard-card-header">
                    <div class="dashboard-card-title">${title}</div>
                    <button class="icon-btn" onclick="controller.removeFromDash('${cId}','${wrapperId}')">×</button>
                </div>
                <div class="dashboard-card-body"><canvas id="dashCanvas-${cId}" class="dashboard-card-canvas"></canvas></div>
            </div>`;

        const tileWrapper = document.createElement('span');
        tileWrapper.id = `graphWrapper_${Date.now()}`;
        tileWrapper.innerHTML = this.obj.parseEvents(chartContent(chart.title, chart.id, tileWrapper.id));
        grid.append(tileWrapper);
     
    }

	removeFromDash(index, wrapperId) {
        const elmToRemoveIdx = this.obj.state.dashboards[this.obj.state.activeDash].findIndex(elm => elm.id == index)
		this.obj.state.dashboards[this.obj.state.activeDash].splice(elmToRemoveIdx, 1);
        document.getElementById(wrapperId).remove();
        BIController.dashboardAddedCharts.delete(index);
        this.obj.state.chartsByDashboard[this.obj.state.activeDash].delete(index);
	}

    openPublishModal() { this.obj.popup.querySelector('#publishModal').classList.add('open'); }
    closePublishModal() { this.obj.popup.querySelector('#publishModal').classList.remove('open'); }

    publishChart() {
        const { state } = this.obj;

        if (!state.pendingChart) return showToast("No chart to publish");
        let dashName = this.obj.popup.querySelector('#newDashInput').value || this.obj.popup.querySelector('#publishDashSelect').value;

        if (!state.dashboards[dashName]) state.dashboards[dashName] = [];

        state.dashboards[dashName].push({...state.pendingChart, instanceId: Date.now()});
        this.closePublishModal();
        this.showToast(`Published to ${dashName}`);
    }

    async newDashboard() {
        const name = prompt("Dashboard Name:");
        if (name) {
            const dashboardList = this.obj.dashboardList.value;
            this.obj.dashboardList = [ ...(Array.isArray(dashboardList) ? dashboardList : []), { dashboard_name: name }];
            //Set the UI selected dashboard as the newly created
            setTimeout(() => document.querySelector('.bi-dashboard-list select').value = name, 200);
            this.obj.state.dashboards[name] = [];
            await this.loadDashboard(name);
        }
    }

    static currentTableList = [];

	async onPipelineChange(val) {
        this.obj.state.pipeline = val;
        let tablesByContext = await BIController.getDomainPipelineFields(val);
        BIController.currentTableList = Object.entries(tablesByContext).map(([name, cols]) => ({ name, cols, totalCols: cols.length }));
        this.obj.state.activeTable = BIController.currentTableList[0].name;
        this.renderTableList();
        this.viewingTables.clear();
	}

    async loadSavedChart(id) {

        const { state } = this.obj;
        
        console.log('THE SAVED CHARTS ARE: ', state.savedCharts);
        
        const c = state.savedCharts['chart-'+id+'n'];
        if (!c) return;

        this.switchTab("chart", document.querySelectorAll(".tab")[1]);
        this.obj.popup.querySelector('.chartTitleInput').value = c.title;
        state.chartType = c.type;
        state.chartColor = c.config.color;

        const { xCol, yCol, agg, imported, viewingTables, dataSource } = c;

        if(imported){
            const loader = this.showChartDataFetchLoading();
            const fields = this.genDuckDBFieldNames(viewingTables);
            await this.runAnaluticsAndRenderSheet(fields, dataSource);
            await sleepForSec(1000);
            delete c.imported;
            loader.hideLoading();
        }

        this.obj.popup.querySelector('#xAxisSelect').value = xCol;
        this.obj.popup.querySelector('#yAxisSelect').value = yCol;
        this.obj.popup.querySelector('#aggSelect').value = agg;

        this.renderChartTypeGrid();
        this.renderColorRow();
        state.pendingChart = c;

        this.buildChart(c);

    }

    genDuckDBFieldNames = (tables) => `COLUMNS('${tables.map(tbl => `^${tbl}`).join('|')}')`;

    showChartDataFetchLoading(){
        const container = this.obj.popup.querySelector('.chart-canvas-wrap');
        container.innerHTML = this.dataProcessLoading('Feching chart data');
        return { hideLoading: () => container.innerHTML = `<canvas id="chartCanvas"></canvas>` };
    }

    setAgentAskMode(question, options = []) {
        const inputEl = this.obj.popup.querySelector('#ai_input');
        const agentUi = this.obj.popup.querySelector('ai_agent_ui');
        const labelEl = this.obj.popup.querySelector('ai_question_label');
        const pillBox = this.obj.popup.querySelector('ai_pills');

        // 1. Hide Input, Show Agent UI
        inputEl.classList.add('e2e-hidden');
        agentUi.classList.remove('e2e-hidden');

        // 2. Set the Question
        labelEl.innerText = question;

        // 3. Render the choice pills (e.g., Column names from AdventureWorks)
        pillBox.innerHTML = ''; 
        options.forEach(opt => {
            const btn = document.createElement('button');
            btn.className = 'e2e-btn-pill';
            btn.innerText = opt;
            btn.onclick = () => {
                // Send the selection back as a new prompt
                this.handleSendMessage(`Selected: ${opt}`);
                this.resetToInputMode();
            };
            pillBox.appendChild(btn);
        });
    }

    removeColumn(colName) {
        if (!colName) return;
        const { state } = this.obj;
        if (confirm(`Permanent action: Are you sure you want to delete the column "${colName}"?`)) {
            this.obj.gridDataSource.forEach(row => delete row[colName]);
            if (state.sortCol === colName) state.sortCol = null;
            if (state.frozenCols.has(colName)) state.frozenCols.delete(colName);
            this.loadTable(state.activeTable);
            this.showToast(`Column "${colName}" removed`);
        }
    }

    shrinkChatLogs(elm, unshrink){
        const hasFirstMessage = this.obj.popup.querySelector('.message-bubble')
        if(hasFirstMessage) this.obj.popup.querySelector('.message-bubble').style.visibility = 'hidden';

        if(elm?.title == this.obj.analyticsChatStateEnum.OPENED || unshrink){
            elm.title = this.obj.analyticsChatStateEnum.CLOSED, elm.innerHTML = '&ndash;';
            this.obj.popup.querySelector('.ai-analytics-chat-logs').style.width = '35%';
            this.obj.popup.querySelector('.ai-analytics-chat-logs').style.height = '270px';
            this.obj.popup.querySelector('.ai-analytics-chat-logs').style.overflowY = 'scroll';
            if(hasFirstMessage) this.obj.popup.querySelector('.message-bubble').style.visibility = 'visible';
            
        }else{
            if(elm?.title)
                elm.title = this.obj.analyticsChatStateEnum.OPENED, elm.innerHTML = '&plus;';
            this.obj.popup.querySelector('.ai-analytics-chat-logs').style.width = '25px';
            this.obj.popup.querySelector('.ai-analytics-chat-logs').style.height = '25px';
            this.obj.popup.querySelector('.ai-analytics-chat-logs').style.overflow = 'hidden';
            if(hasFirstMessage) this.obj.popup.querySelector('.message-bubble').style.visibility = 'hidden';
        }
    }

    analyticsQuery = '';
    async submitAIAnalyticsQuery(e){        
        if(e.key === 'Enter'){

            AIUtil.aiAgentFlow = AIUtil.AgentFlowType.ANALYTICS;
            let mainContainer = this.obj.popup.querySelector('.ai-analytics-chat-logs'), content;
            
            this.createMessageBubble(this.analyticsQuery, 'user', mainContainer);
            this.shrinkChatLogs(this.obj.popup.querySelector('.minimize-analytics-log'), true);

            this.createMessageBubble(AIUtil.loadingContent(), 'agent', mainContainer);

            let { result, error } = await this.sendDataQueryAgentMessage(this.analyticsQuery);

            if((result?.result || '').includes('CLARIFY:') || result?.answer == 'schema-clarification') content = result.result;
            else if(result?.result == '[]') content = 'Your request didn\'t match any of existing data';
            else content = error ? `${result?.result}` : 'Result rendered in the Data visualization'
            
            AIUtil.setAgentLastMessage(content, null, false, mainContainer);

            await this.obj.setData(error ? [] : JSON.parse(result?.result)).init();
            AIUtil.aiAgentFlow = null;

        }
        this.analyticsQuery = e.target.value;
    }

	createMessageBubble(text, role, mainContainer) {
		AIUtil.createMessageBubble(text, role, null, mainContainer);
		AIUtil.scrollToBottom(false, mainContainer);
	}

    /** @returns { { result: { result } } } */
    sendDataQueryAgentMessage = async(message) => BIService.sendDataQueryAgentMessage(message);

    /** @returns { { result: { result } } } */
    sendAnalyticsRequest = async (fields, pipeline) => BIService.sendAnalyticsRequest(fields, pipeline || this.obj.state.pipeline);

    static getDashboardDetails = async () => BIService.getDashboardDetails();
    static getDomainPipelineFields = async (pipeline) => BIService.getDomainPipelineFields(pipeline)

    checkScroll(el) {
        const arrow = this.obj.popup.querySelector('#field-scroll-arrow');
        const isScrollable = el.scrollHeight > el.clientHeight;
        const isAtBottom = el.scrollHeight - el.scrollTop <= el.clientHeight + 10;

        if (isScrollable && !isAtBottom) arrow.style.display = 'block';
        else arrow.style.display = 'none';
    }

    dataProcessLoading(message){
        return `
            <div class="lab-loader">
                <div class="analytics-dataload-spinner"></div>
                <div style="margin-left:10px; font-weight:bold; color:var(--spinner-top);">${message || 'Recalculating rows'}...</div>
            </div>
        `;
    }

    async saveChartConfig(configsFromPivot) {
        const pipeline = this.obj.state.pipeline.split('.')[1];
        const configs = configsFromPivot || JSON.parse(JSON.stringify(this.obj.state.pendingChart)) || {};
        if(!configsFromPivot) configs.config.values = [];
        const chartId = configs?.type == 'pivotTable' ? `pivot-${configs?.id}` :  configs?.id
        return await BIService.saveChartConfig(JSON.stringify(configs), pipeline, configs?.title, configs?.dataSource, chartId);
    }

    async saveDashboardConfig() {

        const name = this.obj.state.activeDash;
        const charts = [...this.obj.state.chartsByDashboard[this.obj.state.activeDash]].filter(chart => chart != null);
        const result = await BIService.saveDashboardConfig(JSON.stringify(charts), name, 0);

        if(result){
            this.showToast("Dashboard saved to library");
            AppTemplate.toast.success('Dashboard saved to library');
        }else{
            AppTemplate.toast.error('Error while saving Dashboard')
            this.showToast("Error while saving Dashboard");
        }
    }

    toggleFilterDrawer(){ this.obj.filterUtil.toggleFilterDrawer(); }
	addCustomGlobalFilter = (field) => this.obj.filterUtil.addCustomGlobalFilter(field);

    openFilter = (e, f, isGlobal = false) => this.obj.filterUtil.openFilter(e, f, isGlobal);

    removeGlobalFilter = (field) => this.obj.filterUtil.removeGlobalFilter(field);

    toggleFilterValueBehavior = (v) => this.obj.filterUtil.toggleFilterValueBehavior(v, true);

    applyGlobalFilters = (v) => this.obj.filterUtil.applyGlobalFilters();

}