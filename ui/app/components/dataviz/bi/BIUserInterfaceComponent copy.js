import { Assets } from "../../../../@still/util/componentUtil.js";
import { UUIDUtil } from "../../../../@still/util/UUIDUtil.js";
import { BIController } from "../../../controller/BIController.js";
import { ModalWindowComponent } from "../../abstract/ModalWindowComponent.js";
import { PopupUtil } from "../../popup-window/PopupUtil.js";
import { Workspace } from "../../workspace/Workspace.js";
import { mockDataTables, mockDepartments, mockTitles } from "./mock.js";
import { BiUiUtil } from "./util.js";

export class BIUserInterfaceComponent extends ModalWindowComponent {

	isPublic = false;

	/** @Prop */ showWindowPopup = false;

	/** @Prop */ uniqueId = '_'+UUIDUtil.newId();

	/** @Prop @type { HTMLElement } */ popup = null;

 	/** @Prop */ CHART_TYPES = BiUiUtil.chartsTypes;
 	/** @Prop */ COLORS = BiUiUtil.chartColors;
 	/** @Prop */ MOCK_TABLES = mockDataTables;
 	/** @Prop */ DEPTS = mockDepartments;
 	/** @Prop */ TITLES = mockTitles;

 	/** @Prop */ 
	state = {
		pipeline:'p1', activeTable:'HumanResources_Employee',
		filteredRows:[], selectedRows:new Set(),
		sortCol:null, sortDir:'asc',
		chartType:'bar', chartColor:COLORS[0],
		chartInstance:null, savedCharts:[],
		dashboards:{'Main Dashboard':[],'Sales Overview':[]},
		activeDash:'Main Dashboard', pendingChart:null,
		frozenCols: new Set(), activeInsertIndex: -1
	};
 	
	MOCK_DATA = null;

  	/** @type { Workspace } */ $parent;

	/** 
	 * @Controller
	 * @Path controller/
	 * @type { BIController }  */
	controller;

	async stOnRender(){
		await Assets.import({ path: '/app/assets/css/bi-user-intercace-component.css' });
	}

  	async stAfterInit(){

		this.popup = document.getElementById(this.uniqueId);
		this.setOnMouseMoveContainer();
		this.setOnPopupResize();
		this.util = new PopupUtil();
		//this.MOCK_DATA = this.genData();

		//this.controller.on('load', () => this.controller.obj = this);
		//console.log(`STARTED THE COMPONENT: `, this.controller);
		
  	}

  	showToast(msg, type='default') {
		const t = document.getElementById('toast');
		t.className = 'toast show ' + type;
		document.getElementById('toastMsg').textContent = msg;
		setTimeout(() => t.classList.remove('show'), 2500);
  	}

	genData() {
		const r = [];
		for (let i = 1; i <= 290; i++)
			r.push({
				BusinessEntityID: i,
				NationalIDNumber: String(Math.floor(Math.random() * 900000000 + 100000000)),
				JobTitle: TITLES[i % TITLES.length] + " - " + DEPTS[i % DEPTS.length],
				Department: DEPTS[i % DEPTS.length],
				HireDate: new Date(2005 + (i % 15), i % 12, (i % 28) + 1).toISOString().split("T")[0],
				VacationHours: Math.floor(Math.random() * 99),
				SickLeaveHours: Math.floor(Math.random() * 69),
				SalariedFlag: i % 3 === 0 ? 0 : 1,
				Gender: i % 2 === 0 ? "M" : "F",
				MaritalStatus: i % 3 === 0 ? "S" : "M",
			});
		return r;
	}

	init() {
		this.controller.renderTableList();
		this.renderChartTypeGrid();
		this.renderColorRow();
		this.loadTable(state.activeTable);
		this.renderDashboardSelect();
		this.renderSavedCharts();
		this.initDragAndDrop();
		this.loadDashboard(state.activeDash);
		this.controller.initInsertLogic();
	}

	onPipelineChange(val) {
		this.state.pipeline = val;
		const tables = MOCK_TABLES[val] || [];
		state.activeTable = tables[0]?.name || "";
		this.controller.renderTableList();
		loadTable(state.activeTable);
	}

	toggleFreeze = (col) => this.controller.toggleFreeze(col);

	sortBy = (col) => this.controller.sortBy(col);

	toggleRow = (i) => this.controller.toggleRow(i);

}








