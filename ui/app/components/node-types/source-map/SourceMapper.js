import { WorkSpaceController } from "../../../controller/WorkSpaceController.js";
import { UserService } from "../../../services/UserService.js";
import { DGServiceController } from "../../governance/controller/DGServiceController.js";
import { AbstractNode } from "../abstract/AbstractNode.js";
import { NodeTypeInterface } from "../mixin/NodeTypeInterface.js";
import { InputConnectionType } from "../types/InputConnectionType.js";

/** @implements { NodeTypeInterface } */
export class SourceMapper extends AbstractNode {

	isPublic = true;

	/** This is strictly to reference the object in the diagram 
	 * @Prop */ nodeId;
	/** @Prop */ label = 'Source Mapper';
	/** @Prop */ showLoading = true;
	/** @Prop */ inConnectors = 1;
	/** @Prop */ outConnectors = 1;
	
	nodeCount = '';
	templatesList;
	templateType = '';
	selectedTemplate;
	privateEndpoint = '';

	/** @Prop */ destinationSettings = {}
	/** @Prop */ aiGenerated;
	/** @Prop */ isImport;
	/** @Prop */ importData = null;
	/** @Prop */ showEndpoint = false;

	async stOnRender(data){		
		const { nodeId, aiGenerated } = data;
		this.nodeId = nodeId;
		this.aiGenerated = aiGenerated;
		this.importData = data;
	}

	async getTemplates(){
		let response = await DGServiceController.getTemplates();
		const { result = {} } = await response.json() || {};
		return result?.result.map(r => ({ name: r[0], type: r[1] })) || [];
	}

	async reloadMe(){
		this.showLoading = true;
		this.templatesList = await this.getTemplates();
		this.selectedTemplate = '';
		this.showLoading = false;
	}

	async stAfterInit(){
	
		this.templatesList = await this.getTemplates();
		this.showLoading = false;

		this.selectedTemplate.onChange(value => {
			if(value.trim() === '') return;
			const selectedTemplate = this.templatesList.value.find(obj => obj.name === value);
			
			WorkSpaceController.getNode(this.nodeId).data['selectedTemplate'] = value;
			WorkSpaceController.getNode(this.nodeId).data['templateType'] = selectedTemplate.type;
			this.templateType = selectedTemplate.type || '';
			
		});

		if(this.importData?.isImport){
			this.disableInputsImpl();
			this.notifyReadiness();
			setTimeout(() => {
				this.selectedTemplate = this.importData.selectedTemplate;
				document.querySelector(`.${this.cmpInternalId}-selected-template-sourcemap`).value = this.importData.selectedTemplate;
			}, 100)
		}

		WorkSpaceController.getNode(this.nodeId).data['namespace'] = await UserService.getNamespace();
	}

	onOutputConnection(){
		SourceMapper.handleOutputConnection(this);
		return { nodeCount: this.nodeCount.value, source: this };
	}

	/** @param { InputConnectionType<{}> } param0 */
	onInputConnection({ type, data }){
		SourceMapper.handleInputConnection(this, data, type);
	}

	onShowEndpoint = () => {
		this.showEndpoint = this.showEndpoint ? false : true;
		
		if(this.showEndpoint) {
			if(this.privateEndpoint.value === '') {
				this.privateEndpoint = '/this-is-the-endpoint';
			}
		}
	}

	onSubmitPreRun(){
		WorkSpaceController.getNode(this.nodeId).data['destinationSetting'] = this.destinationSettings;
	}

}