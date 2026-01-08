import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { WorkSpaceController } from "../../../controller/WorkSpaceController.js";
import { UserService } from "../../../services/UserService.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { AbstractNode } from "../abstract/AbstractNode.js";
import { NodeTypeInterface } from "../mixin/NodeTypeInterface.js";
import { InputConnectionType } from "../types/InputConnectionType.js";

/** @implements { NodeTypeInterface } */
export class InputAPI extends AbstractNode {

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
	nodeCount = '';

	/** @Prop */ aiGenerated;
	/** @Prop */ isImport;
	/** @Prop */ importData = null;

	async stOnRender(data){
		const { nodeId, aiGenerated } = data;
		this.nodeId = nodeId;
		this.aiGenerated = aiGenerated;
		this.importData = data;
	}

	async reloadMe(){
		this.showLoading = true;
		this.secretsList = await WorkspaceService.listSecrets(2);
		this.selectedSecret = '';
		this.showLoading = false;
	}

	async stAfterInit(){
		this.secretsList = await WorkspaceService.listSecrets(2);
		this.showLoading = false;

		this.selectedSecret.onChange(value => {
			if(value.trim() === '') return;
			const selectedSecret = this.secretsList.value.find(obj => obj.name === value);
			
			WorkSpaceController.getNode(this.nodeId).data['connectionName'] = value;
			WorkSpaceController.getNode(this.nodeId).data['baseUrl'] = selectedSecret.host;
			this.host = selectedSecret.host || '';
			this.totalEndpoints = selectedSecret.totalEndpoints || '';
		});

		if(this.aiGenerated && this.importData.connectionName) this.selectedSecret = this.importData.connectionName;

		if(this.importData?.isImport){	
			this.notifyReadiness();		
			this.host = this.importData.baseUrl;
			this.selectedSecret = this.importData.connectionName;
			const selectedSecret = this.secretsList.value.find(obj => obj.name === this.selectedSecret.value);
			this.totalEndpoints = selectedSecret.totalEndpoints || '';
		}

		WorkSpaceController.getNode(this.nodeId).data['namespace'] = await UserService.getNamespace();
	}

	onOutputConnection(){
		InputAPI.handleOutputConnection(this);
		return { nodeCount: this.nodeCount.value };
	}

	/** @param { InputConnectionType<{}> } param0 */
	onInputConnection({ type, data }){
		InputAPI.handleInputConnection(this, data, type);
	}

}