import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { State } from "../../../@still/component/type/ComponentType.js";
import { Assets } from "../../../@still/util/componentUtil.js";
import { AppTemplate } from "../../../config/app-template.js";
import { DGServiceController } from "../governance/controller/DGServiceController.js";

/**
 * Lists every operation that ran against a mapping template (created, updated, executed, ...),
 * with filters, paging and a details dialog.
 *
 * Data source: this.serviceController.getTemplateOperations(templateName)
 * (see fetchOperations() for the expected response). While that method does not exist in
 * DGServiceController, sample data is shown, with a notice, so the screen can be reviewed.
 */
export class OperationsDisplay extends ViewComponent {

	isPublic = true;

	/** @Prop @type { HTMLElement } */ container;

	/** @Prop */ loading = true;

	/** Optional: pre-select a template (can be passed by the parent). Empty = all templates */
	/** @Prop */ templateName = '';

	/** @type { State<String> } */ summary = '';

	/**
	 * @Inject @Path components/governance/controller/
	 * @type { DGServiceController }
	 */
	serviceController;


	/** @returns { HTMLElement } */ $ = (ref) => this.container.querySelector(ref);

	async stBeforeInit() {
		await Assets.import({ path: '/app/components/pipeline/styles/shared.css', type: 'css' });
	}

	async stAfterInit() {
        
	}

}