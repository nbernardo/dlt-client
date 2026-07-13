import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { ListState } from "../../../@still/component/type/ComponentType.js";
import { Router } from "../../../@still/routing/router.js";
import { Components } from "../../../@still/setup/components.js";
import { AppTemplate } from "../../../config/app-template.js";
import { WorkSpaceController } from "../../controller/WorkSpaceController.js";
import { PipelineService } from "../../services/PipelineService.js";
import { UserService } from "../../services/UserService.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";
import { StringUtil } from "../../util/StringUtil.js";
import { UserUtil } from "../auth/UserUtil.js";
import { PipelineRunSummary } from "../pipeline/summary/PipelineRunSummary.js";
import { handleHideShowSubmenu } from "../workspace/generic-util.js";
import { Workspace } from "../workspace/Workspace.js";

export class Header extends ViewComponent {

	isPublic = true;

	/** 
	 * @Inject @Path services/ 
	 * @type { UserService }*/
	userService;
	
	/** 
	 * @Inject @Path services/ 
	 * @type { WorkspaceService }*/
	workspaceService;

	/**
	 * @Inject @Path controller/
	 * @type { WorkSpaceController }
	 */
	workspaceController;

	scheduledPipelinesCount = -1;

	/** @type { ListState<Array> } */
	scheduledPipelines = [];

	loggedUser = null;

	/** @Prop */ showLogsIcon = true;
	/** @Prop */ showScheduleCounter = false;

	/** @type { Workspace } */
	$parent;

	stAfterInit(){
		
		this.userService.on('load', async () => {

			let user = (await this.userService.getLoggedUser());

			if(user?.user)
				user = user?.user;

			if(UserUtil.name === null){
				UserUtil.name = user.name;
				UserUtil.email = user.email;
				Object.freeze(UserUtil);
			}
			this.loggedUser = user.name, this.userEmail = user.email;

		});

		this.scheduledPipelines.onChange(val => this.scheduledPipelinesCount = val.length || 0);

		this.workspaceService.on('load', async () => {
			await this.getInitData();
			this.showScheduleCounter = true;
		});

		this.workspaceController.on('load', () => this.workspaceController.activeHeader = this);
		this.handleScheduledPplineHideShow();
	}

	async getScheduleList(){
		const scheduledPipelinesInitList = await WorkspaceService.getPipelineSchedules();
		this.workspaceService.schedulePipelinesStore = scheduledPipelinesInitList.data;			
		this.scheduledPipelines = (scheduledPipelinesInitList || []).map(itm => 
			({ pipelineLbl: JSON.parse(itm.schedule_settings).ppline_label, ...itm, lastRun: itm.last_run == null ? 'None' : itm.last_run })
		);			
		this.scheduledPipelinesCount = scheduledPipelinesInitList.length || 0;
	}

	async getInitData(){

		const namespaceInitData = await WorkspaceService.getPipelineInitialData();
		
		this.$parent.extentionWarning = !namespaceInitData.is_lancedb_on_duckdb;		
		this.$parent.extentionWarning = !namespaceInitData.is_lancedb_on_duckdb;		
		this.workspaceService.aiAgentNamespaceDetails = namespaceInitData.ai_agent_namespace_details;
		this.workspaceService.schedulePipelinesStore = Object.values(namespaceInitData.schedules.data);			
		this.scheduledPipelines = this.workspaceService.schedulePipelinesStore.value.map(itm =>
			({ pipelineLbl: JSON.parse(itm.schedule_settings).ppline_label, ...itm, lastRun: itm.last_run == null ? 'None' : itm.last_run })
		);
		this.scheduledPipelinesCount = this.workspaceService.schedulePipelinesStore.value.length;
		this.workspaceService.totalPipelines = namespaceInitData.total_pipelines;

	}

	navigateTo = (routeName) => {
		if(routeName == Router.getCurrentViewName())
			return
		this.workspaceController.startedAgent = null;
		AppTemplate.showLoading();
		Router.goto(routeName);
	}
	
    handleScheduledPplineHideShow = () =>  
		document.querySelectorAll('.generic-context-drop-menu').forEach(elm => handleHideShowSubmenu(elm, '.submenu'));

	showHideLogsDisplay = () => this.$parent.logProxy.showHideLogsDisplay();

	logout = () => this.userService.logOut();

	showLogsAnalysisDisplay = () => this.$parent.logQueryDisplayProxy.openPopup();

	async showPipelineRuns(){
		AppTemplate.showLoading();

		const parentId = this.$parent.cmpInternalId;
		let runsList = await PipelineService.getPipelinesRunHistory();
		runsList = (runsList || []).map(this.parseRunListResult);
		const { template: uiContent, component } = await Components.newView(PipelineRunSummary, { }, parentId);
		this.$parent.dynamicViewPlaceholder.innerHTML = uiContent;
		await sleepForSec(1000);
		AppTemplate.hideLoading();
		
		component.runList = runsList;
	}
	
	parseRunListResult(row){

		let { start_time: startTime, update_time: upTime } = row;
		
		row.start_time = new Date(startTime * 1000).toISOString().split('.')[0].replace('T',' '), 
		row.update_time = new Date(upTime * 1000).toISOString().split('.')[0].replace('T',' ');
		row.pipelineUniqueName = row.pipeline;
		row.pipeline = StringUtil.snakeToCamel(row.pipeline);

		return row;

	}
}