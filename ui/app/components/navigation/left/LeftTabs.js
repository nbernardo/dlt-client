import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { UUIDUtil } from "../../../../@still/util/UUIDUtil.js";
import { StillTreeView } from "../../../../@still/vendors/treeview/StillTreeView.js";
import { AppTemplate } from "../../../../config/app-template.js";
import { UserService } from "../../../services/UserService.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { getSupportedQueryDestinations, nonDuckDBSupport } from "../../../services/DestinationUtil.js";
import { FileList } from "../../filelist/FileList.js";
import { FileUpload } from "../../fileupload/FileUpload.js";
import { dbIcon, pipelineIcon, tableIcon, tableIconOpaqued } from "../../workspace/icons/database.js";
import { Workspace } from "../../workspace/Workspace.js";
import { PipelinePlanService } from "../../dataviz/services/PipelinePlanService.js";
import { WorkSpaceController } from "../../../controller/WorkSpaceController.js";
import { PipelineService } from "../../../services/PipelineService.js";
import { StillAppSetup } from "../../../../config/app-setup.js";

const tabToId = { 'content-data-source': 1, 'content-api-catalog': 2, 'content-pipeline-plan': 3, 'content-outputs': 4 };

export class LeftTabs extends ViewComponent {

	isPublic = true;

	/**
	 * @Inject @Path services/
	 * @type { WorkspaceService } */
	service;

	/** @Proxy @type { StillTreeView } */
	dbTreeviewProxy;

	/** @Proxy @type { StillTreeView } */
	arhivePplineTreeviewProxy;

	/** @Proxy @type { FileUpload } */
	fileUploadProxy;

	/** @Proxy @type { FileList } */
	fileListProxy;
	
	/** @Proxy @type { FileList } */
	scriptListProxy;

	objectTypes;

	selectedTab = null;

	/** @type { Workspace } */ $parent;

	/** @Prop */ fileMenu;

	/** @Prop */ promptSamplesMenu;

	/** @Prop */ activeFileDropdown;
	
	/** @Prop */ fetchingPipelineData = false;

	/** @Prop */ uniqPromptMenuId = '_'+UUIDUtil.newId();

	/** @Prop */ selectedPrompt = null;

	/** @Prop */ showLoading = false;

	/** @Prop */ currentDBFile;

	/** @Prop */ currentTableName;
	
	/** @Prop */ currentTableToQuery;

	/** @Prop */ onlyScheduledPplineFilter = false;

	/** @Prop */ _3ViewProxy = null;

	dataFetchilgLabel = 'Fetching Data';
	dbSecretsList = [];
	apiSecretsList = [];
	pipelinePlanList = [];
	pipelines = [];

	stAfterInit() {
		this.$parent.controller.leftTab = this, this.setUpPromptMenuEvt();
		this.service.on('load', () => this.objectTypes = this.service.objectTypes);
	}

	async callShowHideDatabase(proxyName){
		await this.showHideDatabase(proxyName);
		this.showLoading = false;
	}
	/** @param { HTMLElement | null } target */
	async showHideDatabase(proxy = 'dbTreeviewProxy'){
		this.dataFetchilgLabel = '', this._3ViewProxy = proxy;
		if(this.fetchingPipelineData == false) this.fetchingPipelineData = true;
		else return; // This will prevent the button to be clicked multiple times

		this.selectTab('content-outputs');
		const /** @type { StillTreeView } */ proxyObject = this[proxy];

		proxyObject.clearTreeData();
		const payload = proxy == 'arhivePplineTreeviewProxy' ? { archived: true } : {};
		let response = await this.service.getPipelines(this.$parent.socketData.sid, payload);
		
		// Store table metadata for later use (including connection info)
		this.tableMetadata = {};
		
		if(response?.no_data || Object.keys(response).length === 0){
			this.dataFetchilgLabel = proxy == 'arhivePplineTreeviewProxy' ? 'No archived Pipeline exist in your namespace.' : 'No Pipeline data exist in your namespace.';
			return this.fetchingPipelineData = false;
		}

		if(response?.error === true){
			proxyObject.showLoader = false;
			for(const err of response.trace) this.$parent.logProxy.appendLogEntry('error', err, Date.now());

			this.$parent.logProxy.lastLogTime = null;
			this.fetchingPipelineData = false;
			return AppTemplate.toast.error(response.message);
		}
		
		for(const [_file, tables] of Object.entries(response)){
			const isScheduledOnly = '_e2e_schedule' in (tables || {});
			const data = Object.values(isScheduledOnly ? {} : tables);
			const dbfile = _file.replace('.duckdb',''), flag = data[0]?.flag, 
				  isScheduled = data[0]?.is_scheduled || isScheduledOnly, 
				  scheduleSettings = isScheduledOnly ? tables['_e2e_schedule']?.short_settings : data[0]?.short_settings,
				  isSchedulePaused = isScheduledOnly ? tables['_e2e_schedule']?.is_scheduled_paused : data[0]?.is_scheduled_paused;
			const ppline3Content = this.pipelineTreeViewTemplate(dbfile, flag, {isScheduled, scheduleSettings, isSchedulePaused});
			const pipeline = proxyObject.addNode({ content: ppline3Content, isTopLevel: true });

			if(flag) continue;

			let dbSchema = null, tableKey;
			if(data[0]){
				const dbSchemaContent = this.dbSchemaTreeViewTemplate(data[0].dbname, dbfile)
				dbSchema = proxyObject.addNode({ content: dbSchemaContent });
			}

			for(const idx in data){
				
				const tableData = data[idx];
				if(tableData){
					const tableToQuery = `${tableData.dbname}.${tableData.table}`, pipelineName = tableData.ppline || dbfile;
					// Construct the correct metadata key based on destination type
					// CRITICAL: Include pipeline name to avoid conflicts when multiple pipelines have same table names
					if (nonDuckDBSupport.includes(tableData.dest)) {
						// For SQL, BigQuery, and Databricks destinations: use ppline.dbname.table format
						tableKey = `${pipelineName}.${tableData.dbname}.${tableData.table}`;
					} else {
						// For DuckDB: use ppline.dbfile.duckdb.tableToQuery format
						tableKey = `${pipelineName}.${dbfile}.duckdb.${tableToQuery}`;
					}
					
					// Store metadata including connection info
					this.tableMetadata[tableKey] = {
						connection_name: tableData.connection_name,
						dest_type: tableData.dest || 'duckdb',
						ppline: pipelineName, dbname: tableData.dbname,
						table: tableData.table,
						dbfile: dbfile  // Store dbfile for DuckDB queries
					};
					
					// Check if destination type is supported for querying
					const supportedDestinations = getSupportedQueryDestinations();
					const isQuerySupported = supportedDestinations.includes(tableData.dest || 'duckdb');
					const content = this.databaseTreeViewTemplate(tableData, tableToQuery, dbfile, isQuerySupported)
					const table = proxyObject.addNode({ content });
					dbSchema.addChild(table);
				}
			}
			if(dbSchema !== null) pipeline.addChild(dbSchema);
		}
		
		proxyObject.renderTree();
		this.fetchingPipelineData = false, this.dataFetchilgLabel = '';
	}

	pipelineTreeViewTemplate(dbfile, flag, schedule){
		let { isScheduled, scheduleSettings, isSchedulePaused } = schedule;
		isScheduled = String(scheduleSettings).includes('None None None') ? false : isScheduled;
		return `<div class="ppline-treeview">
					<span tooltip-x="0" tooltip-y="-15" tooltip="${dbfile}" 
						class="ppline-treeview-label scheduled-${isScheduled}" style="${flag != undefined ? 'color: orange': ''};"> ${pipelineIcon} <div>${dbfile}</div></span>
				</div>
				<span class="pipeline-menu-holder">
					${isScheduled ? `<span tooltip-x="-190" tooltip="Pipeline schedule for ${scheduleSettings}"><i class="fas fa-clock" style="color: ${isSchedulePaused === 'paused' ? '#ced0cecd;' : '#008000ac;'}"></i></span>` : ''}
					<!-- <img class="scheduled-pipeline-icone" src="app/assets/imgs/file-list/dots.svg" width="12"> -->
					<img class="dots pipeline-menu-dots pipeline-${dbfile}" src="app/assets/imgs/file-list/dots.svg" 
						 onclick="self.showPipelineOptions($event,'${dbfile}',${isScheduled}, '${isSchedulePaused}')" width="12">
					<div class="pipeline-menu-wrapper pipeline-menu-wrap-${dbfile}"></div>
				</span>
				${flag != undefined ? '<span class="pipeline-locked">In use by another proces/job, try after completion.<span>': ''}
				`;
	}

	dbSchemaTreeViewTemplate = (dbname) => `<div class="table-in-treeview"><span> ${dbIcon} <b>${dbname}</b></span></div>`;
	
	copyToClipboard = () => this.$parent.controller.copyToClipboard(this.currentTableName);

	queryTable = () => {
		// Get metadata for current table
		const tableKey = this.currentTableToQuery;
		const metadata = this.tableMetadata?.[tableKey] || {};
		
		this.$parent.expandDataTableView(null, this.currentTableToQuery, this.currentDBFile, null, metadata);
	}
	
	refreshTree = async () => await this.callShowHideDatabase();

	databaseTreeViewTemplate(tableData, tableToQuery, dbfile, showIcons = true){
		let tableRow = `<div class='table-name'>${showIcons ? tableIcon : tableIconOpaqued} ${tableData.table}</div>`;
		let cleanTableName = tableToQuery.replace(/\./g,'_');
		const pipelineName = tableData.ppline || dbfile;
		
		// Construct the correct identifier based on destination type
		// CRITICAL: Include pipeline name to avoid conflicts
		let tableIdentifier;
		if (nonDuckDBSupport.includes(tableData.dest)) {
			// For SQL, BigQuery, and Databricks destinations: use ppline.dbname.table format
			tableIdentifier = `${pipelineName}.${tableData.dbname}.${tableData.table}`;
		} else {
			// For DuckDB: use ppline.dbfile.duckdb.tableToQuery format
			tableIdentifier = `${pipelineName}.${dbfile}.duckdb.${tableToQuery}`;
		}
		
		if(showIcons === true) {
			tableRow += `
				<span onclick="self.showTableOptions('${cleanTableName}','${dbfile}','${tableToQuery}', '${tableIdentifier}')"
					class="pipeline-menu-holder pipeline-menu-holder-table">
					<img class="dots pipeline-menu-dots ${dbfile}${cleanTableName}" src="app/assets/imgs/file-list/dots.svg" width="12">
					<div class="pipeline-table-menu-wrapper pipeline-table-menu-wrap-${dbfile}${cleanTableName}"></div>
				</span>
				`;
		}
		return `<div class="table-in-treeview">${tableRow}</div>`;
	}

	genInitialDBQuery = (table, dbfile) => this.$parent.genInitialDBQuery(table, dbfile)

	openSecretForm = async (secretName, secretType, isBucket, host) => {
		
		if(!secretName || !secretType) this.$parent.controller.catalogForm.showDialog(true, secretType);
		else {
			const data = await WorkspaceService.fetchSecret(secretName, secretType);
			this.$parent.controller.catalogForm.editSecret(secretType, {...data, secretName, isBucket: isBucket == 'yes', host});
		}
		if(secretType === 'api') this.$parent.controller.catalogForm.showTestConnection = true;
	}

	loadGovTablesByPipeline = async (pipelineName) =>
		await this.$parent.controller.governanceView.loadTablesByPipeline(pipelineName);

	async selectTab(tab){
		this.$parent.dynamicViewPlaceholder.innerHTML = '';
		if(tab === 'content-data-governance'){
			AppTemplate.showLoading(StillAppSetup.config.bundle('gov.loadDGUIMsg'));
			this.pipelines = await PipelineService.getPipelinesForGernanceView();
			this.$parent.controller.createDataGovernanceUI();
		}

		this.showLoading = tabToId[tab];
		if(tab === 'content-data-source')
			setTimeout(async () => await this.$parent.controller.createCatalogForm(1),100);

		if(tab === 'content-api-catalog')
			setTimeout(async () => await this.$parent.controller.createCatalogForm(2),100);

		if(tab === 'content-data-files'){
			this.fileListProxy.noFilesMessage = 'No data file found';
			const data = await this.fileUploadProxy.listFiles();
			this.fileListProxy.filesList = data?.length > 0 ? data.map((file, idx) => ({...file, id: 'file'+idx, category: 'data'})) : [];
			this.fileListProxy.setUpFileMenuEvt();
		}

		if(tab === 'content-ppline-script'){
			this.scriptListProxy.noFilesMessage = 'No pipeline script found';
			const data = await this.getPplineFiles();

			if(data){
				const isVersionFile = /\_v[0-9]{1,}\.py$/; //e.g. filename_v1, filename_v2
				let currentFileObject = null, count = 0;
				for (const file of data){
					if(file.name.match(isVersionFile) === null){
						file.id = ++count, currentFileObject = file, currentFileObject.versions = [], currentFileObject.category = 'script';
					}else{
						file.version = true, file.category = 'script', currentFileObject.versions.push(file);
					}
				}
				this.scriptListProxy.filesList = data, this.scriptListProxy.setUpFileMenuEvt();
			}
		}

		if(tab === 'content-pipeline-plan') {
			await this.$parent.controller.createModelDeclarationUI();
			this.pipelines = await PipelineService.getPipelinesForGernanceView();
			//const plans = await PipelinePlanService.getPipelinePlans();
			//this.pipelinePlanList = plans.result, this.showLoading = false;
		}
		this.$parent.selectedLeftTab = tab;
	}

	async getPplineFiles(){
		const ppLinefiles = await this.$parent.service.listPplineFiles(await UserService.getNamespace());
		if(ppLinefiles == null) return null;
		else return ppLinefiles;
	}

	async getPipelinePlan(planId){
		const self = this;
		this.$parent.resetWorkspace(false, async () => {
			AppTemplate.showLoading();
			const plan = await PipelinePlanService.getPlanById(planId);
			WorkSpaceController.pipelinePlanId = plan.id, self.$parent.activeGrid = plan.pipelineName;
			WorkSpaceController.fromContext().processImportingNodes(plan.planContent, false, true);			
			if(plan.processed == 1) self.$parent.showSaveButton = false;
			AppTemplate.hideLoading();
		});
	}

	async viewScript(){ this.$parent.popupWindowProxy.showWindowPopup = true; }

	/** @template */
	async openScriptOnEditor(){}

	/** @template */
	async openDataFileOnEditor(){}

	/** @template */
	viewPipelineDiagram(event, dbfile, asTemplate){}

	showPipelineOptions(event, dbfile, isScheduled, isSchedulePaused){
		event.preventDefault();
		this.currentDBFile = dbfile;
		this.renderDropDownMenu(dbfile, isScheduled, isSchedulePaused);
		const archiveOption = document.querySelectorAll(`.pipeline-menu-wrap-${dbfile} .hide-from-archv-opt`);
		if(archiveOption){
			if (this._3ViewProxy == 'arhivePplineTreeviewProxy') archiveOption.forEach(it => it.style.display = 'none');
			else archiveOption.forEach(it => it.style.display = '');
		}
	}

	showTableOptions(table, dbfile, tableName, tablePath){
		
		this.currentDBFile = dbfile, this.currentTableToQuery = tablePath, this.currentTableName = tableName;
		this.currentTableData = { table, dbfile, tableName, tablePath }; // Store for later use

		const content = document.querySelector('.pipeline-submenu-contents-for-table').innerHTML;
		const target = document.querySelector(`.pipeline-table-menu-wrap-${dbfile}${table}`);
		target.style.display = '', target.innerHTML = content;
		document.addEventListener('click', (event) => {
			if(event.target.classList.contains(`${dbfile}${table}`) || event.target.classList.contains('stop-pipeline-job-icon')) return;
			target.style.display = 'none';
		});
		
	}

	renderDropDownMenu(dbfile, isScheduled, isSchedulePaused){
		WorkspaceService.currentSelectedPpeline = dbfile;
		const content = document.querySelector('.pipeline-submenu-contents').innerHTML;
		const target = document.querySelector(`.pipeline-menu-wrap-${dbfile}`);
		target.style.display = '';
		target.innerHTML = content;
		document.addEventListener('click', (event) => {
			if(event.target.classList.contains(`pipeline-${dbfile}`)) return;
			target.style.display = 'none';
		});
		if(isScheduled) {
			target.querySelector('.scheduled-pipeline-menu-option').style.display = '';
			WorkspaceService.currentSelectedPpelineStatus = isSchedulePaused;
			const label = isSchedulePaused === 'paused' ? 'Resume' : 'Pause';
			const delLabel = isSchedulePaused !== 'paused' ? 'Resume' : 'Pause';
			const button = target.querySelector('.stop-pipeline-job-icon');
			button.classList.remove(`stop-pipeline-job-icon-${delLabel}`), button.classList.add(`stop-pipeline-job-icon-${label}`);
			target.querySelector('.stop-pipeline-job-icon').textContent = label;
		}
	}

	doNothing = (event) => event.preventDefault();

	pauseOrResumePipelineJob = async () => {
		const result = await this.service.pausePipelineScheduledJob();
		if(this.onlyScheduledPplineFilter)
			document.querySelector('.filterSchedulePPlineToggle').checked = false;
		if(result === true) await this.callShowHideDatabase();
	}

	async startAIAssistant(retry = false){
		this.selectTab('content-ai'); 
		await this.$parent.controller.startAgent(retry);
		setTimeout(() => document.getElementById('ai-chat-user-input').focus());
	}

	setNewPrompt = (content) => this.$parent.controller.startedAgent.setUserPrompt(content)
	
	togglePromptPopup(element,isContent = false) {		
		const rect = element.getBoundingClientRect();
		
		if(isContent) this.pasteToAgent(element.innerHTML);
		else this.selectedPrompt = element.parentElement.parentElement.querySelector('.tiny-content').innerHTML;
		
		if (this.activeFileDropdown === element) {
			this.promptSamplesMenu.classList.remove('is-active');
			this.activeFileDropdown = null;
		} else {
			this.promptSamplesMenu.classList.remove('is-active');
			this.promptSamplesMenu.style.left = `${rect.left - 8}px`;
			this.promptSamplesMenu.style.top = `${rect.top}px`;
			this.promptSamplesMenu.classList.add('is-active');
			this.activeFileDropdown = element;
		}
	}

	setUpPromptMenuEvt(){
		this.promptSamplesMenu = document.getElementById(this.uniqPromptMenuId);

		const obj = this; //Becuase inside callback this is not available
        document.addEventListener('click', function(event) {
            const [isClickInsideMenu, isClickTrigger] = [obj.promptSamplesMenu?.contains(event.target), event.target?.closest('img')];
            if (!isClickInsideMenu && !isClickTrigger) {
                obj.promptSamplesMenu?.classList.remove('is-active');
                obj.activeFileDropdown = null;
            }
        });
	}

	pasteToAgent(content = null){
		this.$parent.controller.startedAgent.setUserPrompt(content || this.selectedPrompt);
		this.hideSelectedPromptMenu();
	}

	pasteToCBoard(){
		this.$parent.controller.copyToClipboard(this.selectedPrompt);
		this.hideSelectedPromptMenu();
	}

	hideSelectedPromptMenu = () => this.promptSamplesMenu.classList.remove('is-active');

	filterPipeline(filter, findAll){
		const filterVal = String(filter).toLowerCase().replace(/\s+/g,'_');
		const pipelineList = document.querySelectorAll(`.ppline-treeview-label`);
		const andFilter = this.onlyScheduledPplineFilter ? false : true;
		for(const pipeline of pipelineList){
			const isScheduled = pipeline.classList.contains('scheduled-true');
			if(!(pipeline.textContent.search(filterVal) < 0) && (isScheduled || andFilter))
				pipeline.parentNode.parentNode.parentNode.parentNode.style.display = '';
			else
				pipeline.parentNode.parentNode.parentNode.parentNode.style.display = 'none';
		}
	}

	filterScriptFile = (name) => this.scriptListProxy.filterFileByName('script',name);
	filterDataFile = (name) => this.fileListProxy.filterFileByName('data', name);
	toggleFilterSchedulePPline = (isChecked) => this.onlyScheduledPplineFilter = isChecked;

	showDataCatalog = (event) => {
		event.preventDefault();
		this.$parent.showDataCatalog();
		this.$parent.dataCatalogUIProxy.onPipelineChange(WorkspaceService.currentSelectedPpeline, true);
	}

	openDataViz = () => this.$parent.dataVizProxy.openPopup();

	async archivePpline(event){
		event.preventDefault();
		const title = 'Archiving pipeline.', self = this;
		const message = `By archiving ${self.currentDBFile} you'll deactivate any existing schedule job. Do you wish to confinue?`;
		this.$parent.controller.showDialog(message, { type: 'confirm', title, onConfirm });
		async function onConfirm(){
			setTimeout(async () =>{
				self.dbTreeviewProxy.clearTreeData();
				self.showLoading = 4; // 4 is the loading for the pipelines listing
				await PipelineService.archivePipeline(self.currentDBFile);
				await self.$parent.headerProxy.getScheduleList();
				await self.refreshTree();
			});
		}
	}
}
