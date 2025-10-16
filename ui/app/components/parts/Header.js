import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { ListState } from "../../../@still/component/type/ComponentType.js";
import { Router } from "../../../@still/routing/router.js";
import { AppTemplate } from "../../../config/app-template.js";
import { WorkSpaceController } from "../../controller/WorkSpaceController.js";
import { UserService } from "../../services/UserService.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";
import { UserUtil } from "../auth/UserUtil.js";
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
			this.loggedUser = user.name;
			this.userEmail = user.email;

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
		this.workspaceService.schedulePipelinesStore = scheduledPipelinesInitList.data;			
		this.scheduledPipelines = scheduledPipelinesInitList.data || [];			
		this.scheduledPipelinesCount = scheduledPipelinesInitList.data.length || 0;
	}

	async getInitData(){

		const namespaceInitData = await WorkspaceService.getPipelineInitialData();

		this.workspaceService.aiAgentNamespaceDetails = namespaceInitData.ai_agent_namespace_details;
		this.workspaceService.schedulePipelinesStore = namespaceInitData.schedules.data;			
		this.scheduledPipelines = this.workspaceService.schedulePipelinesStore.value;			
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

    handleScheduledPplineHideShow = () =>  handleHideShowSubmenu('.generic-context-drop-menu', '.submenu');

	showHideLogsDisplay = () => this.$parent.logProxy.showHideLogsDisplay();

	logout(){
		this.userService.logOut();
	}

}