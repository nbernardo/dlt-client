import { WorkSpaceController } from "../../../controller/WorkSpaceController.js";
import { UserService } from "../../../services/UserService.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { DGServiceController } from "../../governance/controller/DGServiceController.js";
import { AbstractNode } from "../abstract/AbstractNode.js";
import { NodeTypeInterface } from "../mixin/NodeTypeInterface.js";
import { InputConnectionType } from "../types/InputConnectionType.js";

/** @implements { NodeTypeInterface } */
export class FileOutput extends AbstractNode {

	isPublic = true;

	/** This is strictly to reference the object in the diagram 
	 * @Prop */ nodeId;
	/** @Prop */ label = 'File Output';
	/** @Prop */ showLoading = true;
	/** @Prop */ inConnectors = 1;
	/** @Prop */ outConnectors = 1;

	outputList = [{ name: 'MFT' }, { name: 'S3' }, { name: 'Local FS'}];
	selectedOutputType = '';
	secretsList;
	selectedSecret = '';
	nodeCount = '';

	/** @Prop */ aiGenerated;
	/** @Prop */ isImport;
	/** @Prop */ importData = null;
	/** @Prop */ showSecretsList = false;

	async stOnRender(data){
		const { nodeId, aiGenerated } = data;
		this.nodeId = nodeId;
		this.aiGenerated = aiGenerated;
		this.importData = data;
	}

	async reloadMe(){
		this.showLoading = true;
		this.secretsList = await WorkspaceService.listSecrets(2);
		this.selectedTemplate = '';
		this.showLoading = false;
	}

	async stAfterInit(){
		this.secretsList = await WorkspaceService.listSecrets(2);
		this.showLoading = false;

		this.selectedOutputType.onChange(value => {
			if(value.trim() === '') return;
			this.showSecretsList = ['MFT','S3'].includes(value) ? true : false;

			const selectedTemplate = this.outputList.value.find(obj => obj.name === value);
			
			WorkSpaceController.getNode(this.nodeId).data['selectedOutputType'] = value;
			WorkSpaceController.getNode(this.nodeId).data['templateType'] = selectedTemplate.type;
			this.templateType = selectedTemplate.type || '';
			
		});

		//if(this.aiGenerated && this.importData.connectionName) this.selectedTemplate = this.importData.connectionName;

		if(this.importData?.isImport){	
			this.notifyReadiness();		
			//this.templateType = this.importData.baseUrl;
			//this.selectedTemplate = this.importData.connectionName;
		}

		WorkSpaceController.getNode(this.nodeId).data['namespace'] = await UserService.getNamespace();
	}

	onOutputConnection(){
		FileOutput.handleOutputConnection(this);
		return { nodeCount: this.nodeCount.value };
	}

	/** @param { InputConnectionType<{}> } param0 */
	onInputConnection({ type, data }){
		FileOutput.handleInputConnection(this, data, type);
	}

	async getDBSecrets(){
		const dbSecretType = 1;	
		this.secretList = (await WorkspaceService.listSecrets(dbSecretType)).filter(itm => itm.host != 'None' && itm.bucket == 'no');
	}

}