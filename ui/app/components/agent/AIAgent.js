import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { Assets } from "../../../@still/util/componentUtil.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";
import { Workspace } from "../workspace/Workspace.js";

export class AIAgent extends ViewComponent {

	isPublic = true;

	/** @type { Workspace } */
	$parent;

	async stBeforeInit(){
		await Assets.import({ path: '/app/assets/css/agent.css' });
	}

	stAfterInit(){
		this.startNewAgent();
	}

	startNewAgent(){
		WorkspaceService.startChatConversation();
	}

	sendChatRequest(event){
		if(event.key === 'Enter'){
			event.preventDefault();
			const message = event.target.value;
			WorkspaceService.sendAgentMessage(message);
		}
	}

}