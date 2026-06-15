import { sleepForSec } from "../../../@still/component/manager/timer.js";
import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { State } from "../../../@still/component/type/ComponentType.js";
import { BIService } from "../dataviz/services/BIService.js";
import { Workspace } from "../workspace/Workspace.js";
import { DataGovernanceController } from "./controller/DataGovernanceController.js";
import { DGServiceController } from "./controller/DGServiceController.js";

export class GovernanceMainComponent extends ViewComponent {

	isPublic = true;

	/**
	 * @Controller @Path components/governance/controller/
	 * @type { DataGovernanceController }
	 */
	controller;

	/** @Prop @type { HTMLElement } */
	container;

	/** @type { State<Array> } */
	pipelines;

	/** @type { Workspace } */
	$parent;

	/** 
	 * @Inject @Path components/governance/controller/ 
	 * @type { DGServiceController }
	 */
	serviceController;

	async stAfterInit(){
		this.container = document.querySelector(`.${this.cmpInternalId}`);
		this.controller.on('load', async () => {
			this.controller.obj = this;
			await sleepForSec(100);
			this.controller.renderAll()
		});

		const pplineList = await BIService.getDWPipelines();
		this.pipelines = (pplineList || []).map(([pipeline, dtset]) => 
			({ pipeline, srcPipeline: dtset.includes('.') ? dtset.split('.')[1] : pipeline })
		)
	}

	loadTablesByPipeline = async(ppline) => {
		const { fields, tables } = await this.serviceController.loadTablesByPipeline(ppline);
		this.controller.pipeline = ppline;
		this.controller.fields = fields;
		this.controller.tables = tables;
		this.controller.renderAll();
	}

	saveDiactionary(){
		const { pipeline, changedFields } = this.controller;
		this.serviceController.savePipelineDisctionary(pipeline, [...(changedFields.values() || [])]);
	}

}



