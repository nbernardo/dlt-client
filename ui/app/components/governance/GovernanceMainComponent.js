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

	/** @type { State<Array> } */
	roles;

	/** @type { Workspace } */
	$parent;

	/** @type { State<String> } */
	accessLevelSummary = '';

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
		this.pipelines = (pplineList || []).map(([pipeline, dtset]) => {
			const path = dtset.split('.');
			const hasPath = path.length > 1;
			return { pipeline, srcPipeline: hasPath ? path[1] : pipeline, dwName: hasPath ? path[0] : dtset }
		})
	}

	loadTablesByPipeline = async(ppline) => {
		const pplinePath = ppline.split('.');
		const { fields, tables } = await this.serviceController.loadTablesByPipeline(pplinePath.slice(0,2).join('.'));
		this.controller.pipeline = ppline;
		this.controller.selectedDw = pplinePath.slice(-1);
		this.controller.fields = fields;
		this.controller.tables = tables;
		this.controller.renderAll();
	}

	saveDiactionary(){
		const { pipeline, changedFields } = this.controller;
		this.serviceController.savePipelineDisctionary(pipeline, [...(changedFields.values() || [])]);
	}

}