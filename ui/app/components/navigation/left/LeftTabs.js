import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { UUIDUtil } from "../../../../@still/util/UUIDUtil.js";
import { StillTreeView } from "../../../../@still/vendors/treeview/StillTreeView.js";
import { AppTemplate } from "../../../../config/app-template.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { FileList } from "../../filelist/FileList.js";
import { FileUpload } from "../../fileupload/FileUpload.js";
import { connectIcon, copyClipboardIcin, dbIcon, pipelineIcon, tableIcon, tableToTerminaIcon, viewpplineIcon } from "../../workspace/icons/database.js";
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
			const dbfile = _file.replace('.duckdb',''), flag = data[0]?.flag;
			const pipeline = this.dbTreeviewProxy.addNode(
				{
					content: this.pipelineTreeViewTemplate(dbfile, flag),
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
						content: this.databaseTreeViewTemplate(tableData, tableToQuery, dbfile),
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

	pipelineTreeViewTemplate(dbfile, flag){
		return `<div class="ppline-treeview">
					<span class="ppline-treeview-label" style="${flag != undefined ? 'color: orange': ''};"> ${pipelineIcon} ${dbfile}</span>
					<span tooltip="Show pipeline diagram" tooltip-x="-160" 
						onclick="self.viewPipelineDiagram($event,'${dbfile}')">${viewpplineIcon}<span>
				</div>
				${flag != undefined ? '<span class="pipeline-locked">In use by another proces/job, try after completion.<span>': ''}
				`;
	}

	dbSchemaTreeViewTemplate(dbname, dbfile){
		return `<div class="table-in-treeview">
					<span> ${dbIcon} <b>${dbname}</b></span>
					<!-- <span onclick="self.connectToDatabase($event, '${dbfile}')">${connectIcon}</span> -->
				</div>`;
	}

	copyToClipboard(content){
		this.$parent.controller.copyToClipboard(content);
	}

	queryTable(dbname, dbfile){
		this.$parent.expandDataTableView(null, dbname, dbfile)
	}

	async refreshTree(){
		await this.showHideDatabase();
	}

	databaseTreeViewTemplate(tableData, tableToQuery, dbfile){
		return `<div class="table-in-treeview">
					<span>${tableIcon} ${tableData.table}</span>
					<span class="tables-icn-container">
						<span tooltip-x="-140" tooltip="Copy table path to clipboard"
							  onclick="self.copyToClipboard('${tableToQuery}')"
						>
							${copyClipboardIcin}
						</span>
						<span 
							onclick="self.queryTable('${dbfile}.duckdb.${tableToQuery}','${dbfile}')"
							tooltip-x="-130" tooltip="Query ${tableData.table} table"
						>
							${tableToTerminaIcon}
						</span>
					</span>
				</div>`;
	}

	genInitialDBQuery(table, dbfile){
		this.$parent.genInitialDBQuery(table, dbfile)
	}

	openSecretForm = async (secretName, secretType) => {
		if(!secretName || !secretType)
			return this.$parent.controller.catalogForm.showDialog(true);
		const data = await WorkspaceService.fetchSecret(secretName, secretType);

		this.$parent.controller.catalogForm.editSecret(secretType, {...data, secretName});
	}

	async selectTab(tab){
		
		if(tab === 'content-data-source'){
			this.showLoading = 1;
			await this.$parent.controller.createCatalogForm(1);
		}

		if(tab === 'content-api-catalog'){
			this.showLoading = 2;
			await this.$parent.controller.createCatalogForm(2);
		}

		if(tab === 'content-data-files'){
			this.fileListProxy.noFilesMessage = 'No data file found';
			const data = await this.fileUploadProxy.listFiles();
			this.fileListProxy.filesList = data?.length > 0 ? data.map((file, idx) => ({...file, id: 'file'+idx})) : [];			
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
					}else{
						file.version = true;
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
		const ppLinefiles = await this.$parent.service.listPplineFiles(this.$parent.userEmail);
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
	viewPipelineDiagram(event, dbfile){}

	async startAIAssistant(retry = false){
		this.selectTab('content-ai'); 
		await this.$parent.controller.startAgent(retry);
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
}
