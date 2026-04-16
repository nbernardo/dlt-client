import { Assets } from "../../../../../@still/util/componentUtil.js";
import { UUIDUtil } from "../../../../../@still/util/UUIDUtil.js";
import { StillAppSetup } from "../../../../../config/app-setup.js";
import { BIChatController } from "../../../../controller/BIChatController.js";
import { BIController } from "../../../../controller/BIController.js";
import { ModalWindowComponent } from "../../../abstract/ModalWindowComponent.js";
import { PopupUtil } from "../../../popup-window/PopupUtil.js";
import { mockDataTables, mockDepartments, mockTitles } from "../mock.js";
import { FilterUtil } from "../pivot/FilterUtil.js";
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

	/**  @Prop  */ showTablesList = true;

	/**  @Prop  */ showDashboardActions = false;

	/** @Prop @type { FilterUtil } */ filterUtil;

	dashboardList = [{ dashboard_name: 'Main Dashboard' }];

	domainPipelinesList = [];

 	/** @Prop */
	state = new State();
 	
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
		//setTimeout(async () => {
			let result = await BIController.getDashboardDetails();
			
			if(result?.error === false && result?.result){
				for(let chart of result?.result.charts){
					chart = JSON.parse(chart);
					if(chart.type === 'pivotTable')
						this.state.savedCharts[`pivot-${chart.id}`] = { ...chart, imported: true };
					else 
						this.state.savedCharts[`chart-${chart.id}`] = { ...chart, imported: true };
				}
				
				if(result?.result?.dashboards.length) {
					this.dashboardList = [];
					this.state.dashboards = [];
				}

				for(const dashboard of result?.result?.dashboards){
					const { charts, dashboard_name } = JSON.parse(dashboard);
					let dataSources = { datasource: new Set(), tables: new Set() };
					this.state.dashboards[dashboard_name] = charts.map(chart => {
						const config = JSON.parse(chart.config);
						
						dataSources.datasource.add(config.dataSource);
						for(const tbl of config.viewingTables) dataSources.tables.add(tbl);

						chart = { ...chart, title: chart.name, config: config.config || config };

						// In case it's pivot table it won't have config.config
						if(!config.config){
							chart = { ...chart, ...chart.config };
							delete chart.config;
						}

						return { ...chart, title: chart.name, config: config.config || config };
					});
					this.dashboardList.push({ dashboard_name });

					dataSources = { datasource: [...dataSources.datasource], tables: [...dataSources.tables] };

					//When reading the dashboard which is saved, the first 2 positions of its array
					// are reserved for the flag imported and the dataSource details respectively  
					this.state.dashboards[dashboard_name] = ['imported', dataSources, ...this.state.dashboards[dashboard_name]]
				}
				this.domainPipelinesList = result?.result?.pipelines?.map(([pp, dbName]) => ({ name: this.toCamel(pp).trim(), pipeline: `${dbName}.${pp}` }));
			}
		//}, 0);
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
		this.filterUtil = new FilterUtil(this);

		this.controller.on('load', () => {
			this.controller.obj = this;
			setTimeout(this.controller.shrinkChatLogs(), 500);
			setTimeout(async () => this.setData(await this.genData()).init(), 500);
		});
		
		this.chatController = new BIChatController(this.popup);
		if(this.runningOnOdoo){
			this.showPopup();
			await this.init();
		}
  	}
	// Mock data for testing
	genData(){
		return [];
		const { TITLES, DEPTS } = this;
		const r=[];for(let i=1;i<=290;i++)r.push({BusinessEntityID:i,NationalIDNumber:String(Math.floor(Math.random()*900000000+100000000)),JobTitle:TITLES[i%TITLES.length]+' - '+DEPTS[i%DEPTS.length],Department:DEPTS[i%DEPTS.length],HireDate:new Date(2005+(i%15),i%12,(i%28)+1).toISOString().split('T')[0],VacationHours:Math.floor(Math.random()*99),SickLeaveHours:Math.floor(Math.random()*69),SalariedFlag:i%3===0?0:1,Gender:i%2===0?'M':'F',MaritalStatus:i%3===0?'S':'M'});
		return r;
	}

  	showToast(msg, type='default') {
		const t = document.getElementById('toast');
		t.className = 'toast show ' + type;
		document.getElementById('toastMsg').textContent = msg;
		setTimeout(() => t.classList.remove('show'), 2500);
  	}

	setData = (dataSource) => {
		this.gridDataSource = dataSource;
		if(this.pivotTableProxy) this.pivotTableProxy.setData(dataSource);
		this.filterUtil.dataset = dataSource;
		return this;
	}

	async init() {
		//this.controller.renderTableList();
		this.controller.renderChartTypeGrid();
		this.controller.renderColorRow();
		this.controller.loadTable(this.state.activeTable);
		this.controller.renderSavedCharts();
		this.controller.initDragAndDrop();
		await this.controller.loadDashboard(this.state.activeDash);
		this.controller.initInsertLogic();
	}

	async openPopup(){
		await this.init();
		this.showPopup();
		this.pivotTableProxy.controller.initSidebar();
	}

	// TODO: Move to a kind of string util
	toCamel = (str) => String(str).replace(/(^|_)([a-z0-9])/g, (_1, _2, group2) => ' '+group2.toUpperCase());;

}


class State {

	pipeline = null; 
	activeTable = null; 
	filteredRows = []; 
	selectedRows = new Set();
	sortCol = null; 
	sortDir = 'asc'; 
	chartType = 'bar'; 
	chartColor = BiUiUtil.chartColors[0];
	chartInstance = null; 
	savedCharts = {}; 
	frozenCols = new Set(); 
	activeInsertIndex = -1;
	dashboards = {'Main Dashboard': [] }; 
	activeDash = 'Main Dashboard'; 
	pendingChart = null;
	/** @type {Array<Set>} */ 
	chartsByDashboard = {}

}