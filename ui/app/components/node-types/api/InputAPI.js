import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { NodeTypeInterface } from "../mixin/NodeTypeInterface.js";

/** @implements { NodeTypeInterface } */
export class InputAPI extends ViewComponent {

	isPublic = true;

	/** @Prop */ label = 'Source - API';
	/** @Prop */ showLoading = true;

	/** @Prop */ inConnectors = 1;
	/** @Prop */ outConnectors = 1;

	secretsList;
	host = '';
	totalEndpoints = '';
	selectedSecret;

	async stAfterInit(){
		this.secretsList = await WorkspaceService.listSecrets(2);
		
		this.showLoading = false;

		this.selectedSecret.onChange(value => {
			const selectedSecret = this.secretsList.value.find(obj => obj.name === value);
			
			this.host = selectedSecret.host || '';
			this.totalEndpoints = selectedSecret.totalEndpoints || '';
		});
	}

}