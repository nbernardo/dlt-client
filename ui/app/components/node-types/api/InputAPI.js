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
	/** @Prop */ showLoading = false;
	/** @Prop */ inConnectors = 1;
	/** @Prop */ outConnectors = 1;
	
	secretsList;
	host = '';
	totalEndpoints = '';
	selectedSecret;
	nodeCount = '';
	selectedApiType = null;

	hookUserName = null;
	hookPassword = null;
	hookEndpoint;
	
	/** @Prop */ apiType = null;
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

		this.selectedSecret.onChange(value => {
			if(value.trim() === '') return;
			const selectedSecret = this.secretsList.value.find(obj => obj.name === value);
			
			WorkSpaceController.getNode(this.nodeId).data['connectionName'] = value;
			WorkSpaceController.getNode(this.nodeId).data['baseUrl'] = selectedSecret.host;
			this.host = selectedSecret.host || '';
			this.totalEndpoints = selectedSecret.totalEndpoints || '';
		});

		this.selectedApiType.onChange(async val => {
			this.showLoading = true, this.apiType = val;
			const inputTypes = val === 'Hook' ? 'inputAPIHook' : 'inputAPIRegular'; 
			const otherInputs = val === 'Hook' ? 'inputAPIRegular' : 'inputAPIHook';

			const inputs = document.querySelector(`.${this.cmpInternalId}`).querySelectorAll(`.${inputTypes} input, .${inputTypes} select`);
			const others = document.querySelector(`.${this.cmpInternalId}`).querySelectorAll(`.${otherInputs} input, .${otherInputs} select`);

			if(val === 'Hook'){
				const hasHook = ['',null,undefined].includes(this.hookEndpoint.value)
				if(hasHook){
					const hookValue = await WorkspaceService.getHookEndpoint();
					this.hookEndpoint = hookValue ? ` /${hookValue}` : null;
				}
				// Only set the hook endpoint in case the value is valid
				if(!hasHook)
					WorkSpaceController.getNode(this.nodeId).data['hookEndpoint'] = val;

			}else if(val === 'Regular'){
				if(['',null,undefined].includes(this.secretsList.value)) 
					this.secretsList = await WorkspaceService.listSecrets(2);
				delete WorkSpaceController.getNode(this.nodeId).data['hookEndpoint'];
			}
			this.showLoading = false;

			inputs.forEach((el) => el.setAttribute('required',true));
			others.forEach((el) => el.removeAttribute('required'));
			WorkSpaceController.getNode(this.nodeId).data['apiType'] = val;

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