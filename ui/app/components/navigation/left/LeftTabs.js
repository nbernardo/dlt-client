import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { UUIDUtil } from "../../../../@still/util/UUIDUtil.js";
import { StillTreeView } from "../../../../@still/vendors/treeview/StillTreeView.js";
import { AppTemplate } from "../../../../config/app-template.js";
import { UserService } from "../../../services/UserService.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { FileList } from "../../filelist/FileList.js";
import { FileUpload } from "../../fileupload/FileUpload.js";
import { dbIcon, pipelineIcon, tableIcon, tableIconOpaqued } from "../../workspace/icons/database.js";
import { Workspace } from "../../workspace/Workspace.js";

export class LeftTabs extends ViewComponent {

	isPublic = true;

	/**
	 * @Inject @Path services/
	 * @type { WorkspaceService } */
	service;

	/** @Proxy @type { StillTreeView } */
	dbTreeviewProxy;

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

	/** @Prop */ underContructionImg = '/app/assets/imgs/bricks.gif';

	/** @Prop */ showLoading = false;

	/** @Prop */ currentDBFile;

	/** @Prop */ currentTableName;
	
	/** @Prop */ currentTableToQuery;

	dataFetchilgLabel = 'Fetching Data';
	dbSecretsList = [];
	apiSecretsList = [];

	stAfterInit() {
		this.$parent.controller.leftTab = this;

		this.setUpPromptMenuEvt();
		this.service.on('load', () => {
			this.objectTypes = this.service.objectTypes;
			this.service.table.onChange(newValue => {
				console.log(`Workspace was update about changed and new value is: `, newValue);
			});
		});
	}

	/** @param { HTMLElement | null } target */
	async showHideDatabase(){
		
		if(this.fetchingPipelineData == false){
			this.fetchingPipelineData = true;
		}else
			return; //This will prevent the button to be clicked multiple times

		this.selectTab('content-outputs');
		this.dbTreeviewProxy.clearTreeData();
		let response = await this.service.getDuckDbs(this.$parent.socketData.sid);
		
		if(response?.no_data || Object.keys(response).length === 0){
			this.dataFetchilgLabel = 'No Pipeline data exist in your namespace.'
			this.fetchingPipelineData = false;
			return;
		}

		if(response?.error === true){
			this.dbTreeviewProxy.showLoader = false;
			for(const err of response.trace) {
				this.$parent.logProxy.appendLogEntry('error', err, Date.now());
			}
			this.$parent.logProxy.lastLogTime = null;
			this.fetchingPipelineData = false;
			return AppTemplate.toast.error(response.message);
		}
		
		for(const [_file, tables] of Object.entries(response)){
			const data = Object.values(tables);
			const dbfile = _file.replace('.duckdb',''), flag = data[0]?.flag, 
				  isScheduled = data[0]?.is_scheduled, 
				  scheduleSettings = data[0]?.short_settings,
				  isSchedulePaused = data[0]?.is_scheduled_paused;

			const pipeline = this.dbTreeviewProxy.addNode({
					content: this.pipelineTreeViewTemplate(dbfile, flag, {isScheduled, scheduleSettings, isSchedulePaused}),
					isTopLevel: true,
			});

			if(flag) continue;

			let dbSchema = null;
			if(data[0]){
				dbSchema = this.dbTreeviewProxy.addNode({
					content: this.dbSchemaTreeViewTemplate(data[0].dbname, dbfile),
				});
			}

			for(const idx in data){
				
				const tableData = data[idx];
				if(tableData){
					const tableToQuery = `${tableData.dbname}.${tableData.table}`;
					const table = this.dbTreeviewProxy.addNode({ 
						content: this.databaseTreeViewTemplate(tableData, tableToQuery, dbfile, !(tableData.dest == 'sql')),
					});
					dbSchema.addChild(table);
				}
			}
			if(dbSchema !== null) pipeline.addChild(dbSchema);
		}
		
		this.dbTreeviewProxy.renderTree();
		this.fetchingPipelineData = false;
		this.dataFetchilgLabel = '';
	}

	pipelineTreeViewTemplate(dbfile, flag, schedule){
		const { isScheduled, scheduleSettings, isSchedulePaused } = schedule;
		return `<div class="ppline-treeview">
					<span class="ppline-treeview-label" style="${flag != undefined ? 'color: orange': ''};"> ${pipelineIcon} <div>${dbfile}</div></span>
				</div>
				<span class="pipeline-menu-holder">
					${isScheduled ? `<span tooltip-x="-190" tooltip="Pipeline schedule for ${scheduleSettings}"><i class="fas fa-clock" style="color: #0080008a;"></i></span>` : ''}
					<!-- <img class="scheduled-pipeline-icone" src="app/assets/imgs/file-list/dots.svg" width="12"> -->
					<img class="dots pipeline-menu-dots pipeline-${dbfile}" src="app/assets/imgs/file-list/dots.svg" 
						 onclick="self.showPipelineOptions($event,'${dbfile}',${isScheduled}, '${isSchedulePaused}')" width="12">
					<div class="pipeline-menu-wrapper pipeline-menu-wrap-${dbfile}"></div>
				</span>
				${flag != undefined ? '<span class="pipeline-locked">In use by another proces/job, try after completion.<span>': ''}
				`;
	}

	dbSchemaTreeViewTemplate = (dbname) =>
		`<div class="table-in-treeview"><span> ${dbIcon} <b>${dbname}</b></span></div>`;
	
	copyToClipboard = () =>
		this.$parent.controller.copyToClipboard(this.currentTableName);

	queryTable = () =>
		this.$parent.expandDataTableView(null, this.currentTableToQuery, this.currentDBFile)
	
	refreshTree = async () => await this.showHideDatabase();

	databaseTreeViewTemplate(tableData, tableToQuery, dbfile, showIcons = true){
		let tableRow = `<div class='table-name'>${showIcons ? tableIcon : tableIconOpaqued} ${tableData.table}</div>`;
		let cleanTableName = tableToQuery.replace(/\./g,'_');
		if(showIcons === true) {
			tableRow += `
				<span
					onclick="self.showTableOptions('${cleanTableName}','${dbfile}','${tableToQuery}', '${dbfile}.duckdb.${tableToQuery}')"
					class="pipeline-menu-holder pipeline-menu-holder-table">
					<img class="dots pipeline-menu-dots ${dbfile}${cleanTableName}" src="app/assets/imgs/file-list/dots.svg" width="12">
					<div class="pipeline-table-menu-wrapper pipeline-table-menu-wrap-${dbfile}${cleanTableName}"></div>
				</span>
				`;
		};

		return `<div class="table-in-treeview">${tableRow}</div>`;
	}

	genInitialDBQuery = (table, dbfile) => this.$parent.genInitialDBQuery(table, dbfile)

	openSecretForm = async (secretName, secretType) => {
		if(!secretName || !secretType)
			return this.$parent.controller.catalogForm.showDialog(true, secretType);
		const data = await WorkspaceService.fetchSecret(secretName, secretType);
		this.$parent.controller.catalogForm.editSecret(secretType, {...data, secretName});
	}

	async selectTab(tab){
		
		if(tab === 'content-data-source'){
			this.showLoading = 1;
			setTimeout(async () => {
				await this.$parent.controller.createCatalogForm(1);
			},100);
		}

		if(tab === 'content-api-catalog'){
			this.showLoading = 2;
			setTimeout(async () => {
				await this.$parent.controller.createCatalogForm(2);
			},100);
		}

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
						file.id = ++count;
						currentFileObject = file;
						currentFileObject.versions = [];
						currentFileObject.category = 'script';
					}else{
						file.version = true;
						file.category = 'script';
						currentFileObject.versions.push(file);
					}
				}
				this.scriptListProxy.filesList = data;
				this.scriptListProxy.setUpFileMenuEvt();
			}
		}
		this.$parent.selectedLeftTab = tab;
	}

	async getPplineFiles(){
		const ppLinefiles = await this.$parent.service.listPplineFiles(await UserService.getNamespace());
		if(ppLinefiles == null) return null;
		else return ppLinefiles;
	}

	async viewScript(){
		this.$parent.popupWindowProxy.showWindowPopup = true;
	}

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
	}

	showTableOptions(table, dbfile, tableName, tablePath){
		
		this.currentDBFile = dbfile;
		this.currentTableToQuery = tablePath;
		this.currentTableName = tableName;

		const content = document.querySelector('.pipeline-submenu-contents-for-table').innerHTML;
		const target = document.querySelector(`.pipeline-table-menu-wrap-${dbfile}${table}`);
		target.style.display = '';
		target.innerHTML = content;
		document.addEventListener('click', (event) => {
			if(event.target.classList.contains(`${dbfile}${table}`) || event.target.classList.contains('stop-pipeline-job-icon')) return;
			target.style.display = 'none';
		});
		
	}

	renderDropDownMenu(dbfile, isScheduled, isSchedulePaused){
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
			WorkspaceService.currentSelectedPpeline = dbfile;
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
		if(result === true) await this.showHideDatabase();
	}

	async startAIAssistant(retry = false){
		this.selectTab('content-ai'); 
		await this.$parent.controller.startAgent(retry);
		setTimeout(() => document.getElementById('ai-chat-user-input').focus());
	}

	setNewPrompt(content){
		this.$parent.controller.startedAgent.setUserPrompt(content)
	}

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

	filderPipeline(filter){
		const filterVal = String(filter).toLowerCase().replace(/\s+/g,'_');
		const pipelineList = document.querySelectorAll('.ppline-treeview-label');
		for(const pipeline of pipelineList){
			if(pipeline.textContent.search(filterVal) < 0)
				pipeline.parentNode.parentNode.parentNode.parentNode.style.display = 'none';
			else
				pipeline.parentNode.parentNode.parentNode.parentNode.style.display = '';
		}
	}

	filterScriptFile = (name) => this.scriptListProxy.filterFileByName('script',name);
	filterDataFile = (name) => this.fileListProxy.filterFileByName('data', name);
}
