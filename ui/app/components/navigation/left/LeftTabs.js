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

	stAfterInit() {

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
		let response = await this.service.getDuckDbs(this.$parent.userEmail, this.$parent.socketData.sid);
		
		if(response?.error === true){
			this.dbTreeviewProxy.showLoader = false;
			for(const err of response.trace) {
				this.$parent.logProxy.appendLogEntry('error', err, Date.now());
			}
			this.$parent.logProxy.lastLogTime = null;
			return AppTemplate.toast.error(response.message);
		}
		
		for(const [_file, tables] of Object.entries(response)){
			const data = Object.values(tables);
			const dbfile = _file.replace('.duckdb','');
			const pipeline = this.dbTreeviewProxy.addNode(
				{
					content: this.pipelineTreeViewTemplate(dbfile),
					isTopLevel: true,
			});

			const dbSchema = this.dbTreeviewProxy.addNode({
				content: this.dbSchemaTreeViewTemplate(data[0].dbname, dbfile),
			});

			for(const idx in data){
				
				const tableData = data[idx];
				const tableToQuery = `${tableData.dbname}.${tableData.table}`;
				const table = this.dbTreeviewProxy.addNode({ 
					content: this.databaseTreeViewTemplate(tableData, tableToQuery, dbfile),
				});
				dbSchema.addChild(table);
			}
			pipeline.addChild(dbSchema);
		}
		
		this.dbTreeviewProxy.renderTree();
		this.fetchingPipelineData = false;
	}

	pipelineTreeViewTemplate(dbfile){
		return `<div class="ppline-treeview">
					<span class="ppline-treeview-label"> ${pipelineIcon} ${dbfile} </span>
					<span tooltip="Show pipeline diagram" tooltip-x="-160" 
						onclick="self.viewPipelineDiagram($event,'${dbfile}')">${viewpplineIcon}<span>
				</div>`;
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
		this.$parent.expandDataTableView(dbname, dbfile)
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

	async selectTab(tab){
		if(tab === 'content-data-files'){
			this.fileListProxy.noFilesMessage = 'No data file found';
			this.fileListProxy.filesList = await this.fileUploadProxy.listFiles();			
			this.fileListProxy.setUpFileMenuEvt();
		}

		if(tab === 'content-ppline-script'){
			this.scriptListProxy.noFilesMessage = 'No pipeline found';
			this.scriptListProxy.filesList = await this.getPplineFiles();
			this.scriptListProxy.setUpFileMenuEvt();
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

	async startAIAssistant(){
		this.selectTab('content-ai'); 
		await this.$parent.controller.startAgent();
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
			
            const [isClickInsideMenu, isClickTrigger] = [obj.promptSamplesMenu.contains(event.target), event.target.closest('img')];
            if (!isClickInsideMenu && !isClickTrigger) {
                obj.promptSamplesMenu.classList.remove('is-active');
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
