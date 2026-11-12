import { sleepForSec } from "../../../@still/component/manager/timer.js";
import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { State, STForm } from "../../../@still/component/type/ComponentType.js";
import { Assets } from "../../../@still/util/componentUtil.js";
import { StillAppSetup } from "../../../config/app-setup.js";
import { AppTemplate } from "../../../config/app-template.js";
import { Workspace } from "../workspace/Workspace.js";
import { CustomMappingController } from "./controller/CustomMappingController.js";
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
	roles;

	/** @type { State<Array> } */
	sourceConnections;

	/** @type { State<string> } */
	templateMappingName;

	/** @type { State<string> } */
	templateConnectionName;

	/** @type { Workspace } */
	$parent;

	/** @type { State<String> } */
	accessLevelSummary = '';

	/** @Prop */ loading = false;

	/** @Prop @type { STForm } */ formRef;

	// Bellow controller is set by the DataGovernanceController
	/** @Prop @type { CustomMappingController } */ mappingController = null;

	/** 
	 * @Inject @Path components/governance/controller/ 
	 * @type { DGServiceController }
	 */
	serviceController;


	async stBeforeInit(){
		await Assets.import({ path: '/app/components/pipeline/styles/shared.css', type: 'css' });
	}

	async stAfterInit(){
		this.container = document.querySelector(`.${this.cmpInternalId}`);
		this.controller.on('load', async () => {
			this.controller.obj = this;
			await sleepForSec(100);
			this.controller.renderAll()
		});
		this.templateConnectionName.onChange(async secretName => await this.mappingController.onConnectionChange(secretName));
	}

	async loadTablesByPipeline(ppline){
		const pplinePath = ppline.split('.');
		const { fields, tables } = await this.serviceController.loadTablesByPipeline(pplinePath.slice(0,2).join('.'));
		this.controller.pipeline = ppline;
		this.controller.selectedDw = pplinePath.slice(-1);
		this.controller.fields = fields;
		this.controller.tables = tables;
		this.controller.renderAll();
	}

	async saveDictionary(){
		this.loading = true;
		const { pipeline, changedFields } = this.controller;
		const result = await this.serviceController.savePipelineDisctionary(pipeline, [...(changedFields.values() || [])]);
		if(result.error)
			AppTemplate.toast.error(result.result)
		else
			AppTemplate.toast.success(StillAppSetup.config.bundle('msg.dictionarySuccess'))
		this.loading = false;
	}

	saveCustomMapping = async () => {
		await this.formRef.validate();		
		if(this.formRef.errorCount > 0) return AppTemplate.toast.error('Fill all required field', 10000);
		await this.mappingController.saveCustomMapping();
	}

}