import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { ListState } from "../../../@still/component/type/ComponentType.js";
import { Router } from "../../../@still/routing/router.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";
import { UserUtil } from "../auth/UserUtil.js";

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

	scheduledPipelinesCount = '...';

	/** @type { ListState<Array> } */
	scheduledPipelines = [];

	loggedUser = null;

	stAfterInit(){
		
		this.userService.on('load', async () => {

			const user = (await this.userService.getLoggedUser());
			if(UserUtil.name === null){
				UserUtil.name = user.name;
				UserUtil.email = user.email;
				Object.freeze(UserUtil);
			}
			this.loggedUser = user.name;
			this.userEmail = user.email;

		});

		this.scheduledPipelines.onChange(val => this.scheduledPipelinesCount = val.length || 0);

		this.workspaceService.on('load', () => {
			this.workspaceService.schedulePipelines = Header.scheduledPipelinesInitList.data;
			this.scheduledPipelinesCount = this.workspaceService.schedulePipelines.value.length;
			this.scheduledPipelines = this.workspaceService.schedulePipelines.value;
		});

		this.handleScheduledPplineHideShow();
	}

	gotoConfig = () => Router.goto('Config');
	
	async stBeforeInit(){
		Header.scheduledPipelinesInitList = await WorkspaceService.getPipelineSchedules();
	}

    handleScheduledPplineHideShow(){
        const scheduleIcon = document.querySelector('.scheduled-pipeline-context-drop-menu');
        const dropMenu = scheduleIcon.querySelector('.submenu');
        scheduleIcon.onmouseover = () => dropMenu.style.display = 'block';

		document.addEventListener('click', (event) => 
			!dropMenu.contains(event.target) ? dropMenu.style.display = 'none' : ''
		);
    }

}