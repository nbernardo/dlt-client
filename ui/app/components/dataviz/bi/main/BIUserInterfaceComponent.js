import { Assets } from "../../../../../@still/util/componentUtil.js";
import { UUIDUtil } from "../../../../../@still/util/UUIDUtil.js";
import { StillAppSetup } from "../../../../../config/app-setup.js";
import { BIChatController } from "../../../../controller/BIChatController.js";
import { BIController } from "../../../../controller/BIController.js";
import { ModalWindowComponent } from "../../../abstract/ModalWindowComponent.js";
import { PopupUtil } from "../../../popup-window/PopupUtil.js";
import { mockDataTables, mockDepartments, mockTitles } from "../mock.js";
import { PivotCreateComponent } from "../pivot/PivotCreateComponent.js";
import { BiUiUtil } from "../util.js";


export class BIUserInterfaceComponent extends ModalWindowComponent {

	isPublic = false;

	/** @Prop */ showWindowPopup = false;

	/** @Prop */ uniqueId = '_'+UUIDUtil.newId();

	/** @Prop @type { HTMLElement } */ popup = null;

 	/** @Prop */ CHART_TYPES = BiUiUtil.chartsTypes;

 	/** @Prop */ COLORS = BiUiUtil.chartColors;

 	/** @Prop */ analyticsRessultTables = mockDataTables;

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
 	
	/** @Prop */ gridDataSource = null;

	/** @Proxy @type { PivotCreateComponent } */ pivotTableProxy = null;

	/** @Prop */ analyticsChatStateEnum = { OPENED: 'Expanded', CLOSED: 'Minimized' };

	/** 
	 * @Controller
	 * @Path controller/
	 * @type { BIController }  */
	controller;

	/** @Prop @type { BIChatController } */ chatController;

	async stBeforeInit(){
		this.runningOnOdoo = StillAppSetup.config.get('runningOnOdoo');
	}

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
		this.gridDataSource = [];
		this.controller.on('load', () => {
			this.controller.obj = this;
			setTimeout(this.controller.shrinkChatLogs(), 500);
		});
		this.chatController = new BIChatController(this.popup);
		if(this.runningOnOdoo){
			this.showPopup();
			this.init();
		}
  	}

  	showToast(msg, type='default') {
		const t = document.getElementById('toast');
		t.className = 'toast show ' + type;
		document.getElementById('toastMsg').textContent = msg;
		setTimeout(() => t.classList.remove('show'), 2500);
  	}

	setData = (dataSource) => {
		this.gridDataSource = dataSource;
		return this;
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