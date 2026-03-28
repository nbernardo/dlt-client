import { Assets } from "../../../../@still/util/componentUtil.js";
import { UUIDUtil } from "../../../../@still/util/UUIDUtil.js";
import { BIChatController } from "../../../controller/BIChatController.js";
import { BIController } from "../../../controller/BIController.js";
import { ModalWindowComponent } from "../../abstract/ModalWindowComponent.js";
import { PopupUtil } from "../../popup-window/PopupUtil.js";
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

	/**  @Prop  */ runningOnOdoo = false;

 	/** @Prop */
	state = {
		pipeline:'p1', activeTable:'HumanResources_Employee',
		filteredRows:[], selectedRows:new Set(),
		sortCol:null, sortDir:'asc',
		chartType:'bar', chartColor: BiUiUtil.chartColors[0],
		chartInstance:null, savedCharts:[],
		dashboards:{'Main Dashboard':[],'Sales Overview':[]},
		activeDash:'Main Dashboard', pendingChart:null,
		frozenCols: new Set(), activeInsertIndex: -1
	};
 	
	/** @Prop */ MOCK_DATA = null;

	/** 
	 * @Controller
	 * @Path controller/
	 * @type { BIController }  */
	controller;

	/** @Prop @type { BIChatController } */ chatController;

	async stOnRender(){
		let cssPathPrefix = '';
		if(this.runningOnOdoo){
			setTimeout(async () => {
				await Assets.import({ path: 'https://cdn.jsdelivr.net/npm/chart.js', type: 'js' });
			});
			cssPathPrefix = `${location.origin}/odoo-e2e-bi/static/src/dashboard-app`;
		}
		await Assets.import({ path: `${cssPathPrefix}/app/assets/css/bi-user-intercace-component.css` });		
	}

  	async stAfterInit(){
		this.popup = document.getElementById(this.uniqueId);
		this.setOnMouseMoveContainer();
		this.setOnPopupResize();
		this.util = new PopupUtil();
		this.MOCK_DATA = this.genData();
		this.controller.on('load', () => this.controller.obj = this);
		this.chatController = new BIChatController(this.popup);
		if(this.runningOnOdoo)
			this.init();
  	}

  	showToast(msg, type='default') {
		const t = document.getElementById('toast');
		t.className = 'toast show ' + type;
		document.getElementById('toastMsg').textContent = msg;
		setTimeout(() => t.classList.remove('show'), 2500);
  	}

	genData() {
		const r = [];
		// This is generating a mock data
		for (let i = 1; i <= 290; i++)
			r.push({
				'BusinessEntityID': i,
				NationalIDNumber: String(Math.floor(Math.random() * 900000000 + 100000000)),
				JobTitle: this.TITLES[i % this.TITLES.length] + " - " + this.DEPTS[i % this.DEPTS.length],
				Department: this.DEPTS[i % this.DEPTS.length],
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
		this.controller.renderChartTypeGrid();
		this.controller.renderColorRow();
		this.controller.loadTable(this.state.activeTable);
		this.controller.renderDashboardSelect();
		this.controller.renderSavedCharts();
		this.controller.initDragAndDrop();
		this.controller.loadDashboard(this.state.activeDash);
		this.controller.initInsertLogic();
	}

	openPopup(){
		this.init();
		this.showPopup();
	}

}