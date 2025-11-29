import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { WorkSpaceController } from "../../../controller/WorkSpaceController.js";
import { UserService } from "../../../services/UserService.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { NodeTypeInterface } from "../mixin/NodeTypeInterface.js";

/** @implements { NodeTypeInterface } */
export class InputAPI extends ViewComponent {

	isPublic = true;

	/** This is strictly to reference the object in the diagram 
	 * @Prop */ nodeId;

	/** @Prop */ label = 'Source - API';
	/** @Prop */ showLoading = true;

	/** @Prop */ inConnectors = 1;
	/** @Prop */ outConnectors = 1;

	secretsList;
	host = '';
	totalEndpoints = '';
	selectedSecret;
	/** @Prop */ isImport;
	/** @Prop */ importData = null;

	async stOnRender(data){
		const { nodeId } = data;
		this.importData = data;
		this.nodeId = nodeId;		
	}

	async stAfterInit(){
		this.secretsList = await WorkspaceService.listSecrets(2);
		this.showLoading = false;

		if(this.importData?.isImport){			
			this.host = this.importData.baseUrl;
			this.selectedSecret = this.importData.connectionName;
			const selectedSecret = this.secretsList.value.find(obj => obj.name === this.selectedSecret.value);
			this.totalEndpoints = selectedSecret.totalEndpoints || '';
		}

		this.selectedSecret.onChange(value => {
			const selectedSecret = this.secretsList.value.find(obj => obj.name === value);
			
			WorkSpaceController.getNode(this.nodeId).data['connectionName'] = value;
			WorkSpaceController.getNode(this.nodeId).data['baseUrl'] = selectedSecret.host;
			this.host = selectedSecret.host || '';
			this.totalEndpoints = selectedSecret.totalEndpoints || '';
		});

		WorkSpaceController.getNode(this.nodeId).data['namespace'] = await UserService.getNamespace();
	}

}