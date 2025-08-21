import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { StillTreeView } from "../../../../@still/vendors/treeview/StillTreeView.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { connectIcon, copyClipboardIcin, dbIcon, pipelineIcon, tableIcon, tableToTerminaIcon, viewpplineIcon } from "../../workspace/icons/database.js";
import { Workspace } from "../../workspace/Workspace.js";

export class LeftTabs extends ViewComponent {

	isPublic = true;

	/**
	 * @Inject
	 * @Path services/
	 * @type { WorkspaceService } */
	service;

	/** @Proxy @type { StillTreeView } */
	dbTreeviewProxy;

	objectTypes;

	selectedTab = null;

	/** @type { Workspace } */
	$parent;

	stAfterInit() {
		this.service.on('load', () => {
			this.objectTypes = this.service.objectTypes;
			this.service.table.onChange(newValue => {
				console.log(`Workspace was update about changed and new value is: `, newValue);
			});
		});
	}

	async showHideDatabase(){
		this.selectTab('content-outputs');
		this.dbTreeviewProxy.clearTreeData();
		let response = await this.service.getDuckDbs();
		response = await response.json();

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
		this.controller.copyToClipboard(content);
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
							onclick="self.genInitialDBQuery('${tableToQuery}','${dbfile}')"
							tooltip-x="-130" tooltip="Query ${tableData.table} table"
						>
							${tableToTerminaIcon}
						</span>
					</span>
				</div>`;
	}

	/** @template */
	viewPipelineDiagram(event, dbfile){}

	selectTab(tab){
		this.$parent.selectedLeftTab = tab;
	}

}