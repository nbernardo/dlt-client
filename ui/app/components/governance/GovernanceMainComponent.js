import { sleepForSec } from "../../../@still/component/manager/timer.js";
import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { DataGovernanceController } from "./controller/DataGovernanceController.js";

export class GovernanceMainComponent extends ViewComponent {

	isPublic = true;

	/**
	 * @Controller @Path components/governance/controller/
	 * @type { DataGovernanceController }
	 */
	controller;

	/** 
	 * @Prop
	 * @type { HTMLElement }
	 * */
	container;

	stAfterInit(){
		this.container = document.querySelector(`.${this.cmpInternalId}`);
		this.controller.on('load', async () => {
			this.controller.obj = this;
			await sleepForSec(100);
			this.controller.renderAll()
		});
	}

}



