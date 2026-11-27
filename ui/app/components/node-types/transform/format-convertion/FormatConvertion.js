import { WorkSpaceController } from "../../../../controller/WorkSpaceController.js";
import { Workspace } from "../../../workspace/Workspace.js";
import { AbstractNode } from "../../abstract/AbstractNode.js";
import { InputAPI } from "../../api/InputAPI.js";
import { NodeTypeInterface } from "../../mixin/NodeTypeInterface.js";
import { InputConnectionType } from "../../types/InputConnectionType.js";

/** @implements { NodeTypeInterface } */
export class FormatConvertion extends AbstractNode {

	isPublic = true;

	/** This is strictly to reference the object in the diagram 
	 * @Prop */ nodeId;
	/** @Prop */ label = 'Format Conversion';
	/** @Prop */ showLoading = false;
	/** @Prop */ showInputs = false;
	/** @Prop */ inConnectors = 1;
	/** @Prop */ outConnectors = 1;
	
	nodeCount = '';

	sourceType = null;
	targetType = null;
	typesOfFormats = [{ name: 'XML' }, { name: 'JSON' }, { name: 'Parquet' }, { name: 'ORC' }]
	
	/** @Prop */ aiGenerated;
	/** @Prop */ isImport;
	/** @Prop */ importData = null;

	/** @type { Workspace } */ $parent;

	async stOnRender(data){
		const { nodeId, aiGenerated } = data;
		this.nodeId = nodeId;
		this.aiGenerated = aiGenerated;
		this.importData = data;
	}

	async stAfterInit(){
		if(this.importData?.isImport){	
			this.notifyReadiness();
		}
	}

	onOutputConnection(){
		FormatConvertion.handleOutputConnection(this);
		return { nodeCount: this.nodeCount.value };
	}

	/** @param { InputConnectionType<{}> } param0 */
	onInputConnection({ type, data }){
		if(type === InputAPI.name) this.showInputs = true;
		FormatConvertion.handleInputConnection(this, data, type);
	}

	onSubmitPreRun(){
		WorkSpaceController.getNode(this.nodeId).data['sourceType'] = this.sourceType.value;
		WorkSpaceController.getNode(this.nodeId).data['targetType'] = this.targetType.value;
	}

	onConectionDelete(){
		this.showInputs = false;
	}


}