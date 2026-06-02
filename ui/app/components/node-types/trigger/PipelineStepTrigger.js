import { WorkSpaceController } from "../../../controller/WorkSpaceController.js";
import { PipelineService } from "../../../services/PipelineService.js";
import { UserService } from "../../../services/UserService.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { Workspace } from "../../workspace/Workspace.js";
import { AbstractNode } from "../abstract/AbstractNode.js";
import { DuckDBOutput } from "../DuckDBOutput.js";
import { NodeTypeInterface } from "../mixin/NodeTypeInterface.js";
import { InputConnectionType } from "../types/InputConnectionType.js";

/** @implements { NodeTypeInterface } */
export class PipelineStepTrigger extends AbstractNode {

	isPublic = false;

	/** This is strictly to reference the object in the diagram 
	 * @Prop */ nodeId;
	/** @Prop */ label = 'Step Trigger';
	/** @Prop */ showLoading = true;
	/** @Prop */ inConnectors = 1;
	/** @Prop */ outConnectors = 1;
	/** @Prop @type { STForm } */ formRef;

	nodeCount = '';
	triggerAfterValues = [];
	pipelineList = [];

	/** @Prop */ aiGenerated;
	/** @Prop */ isImport;
	/** @Prop */ importData = null;
	/** @Prop */ leaderDataSource = null;
	/** @Prop */ showOptions = false;

	/** @type { Workspace } */ $parent;

	async stOnRender(data){
		const { nodeId, aiGenerated } = data;
		this.nodeId = nodeId;
		this.aiGenerated = aiGenerated;
		this.importData = data;
	}

	async filterRelated(status){
		if(status)
			return this.pipelineList = PipelineService.storePipelineShortList.filter(it => it.relateTo == this.leaderDataSource);
		this.pipelineList = PipelineService.storePipelineShortList;
	}

	async stAfterInit(){
		this.showLoading = false;
		this.triggerAfterValues = Array.from({ length: 60 }, (_, label) => ({label: label + 1}));
		this.pipelineList = (await PipelineService.getPipelinesShortList());

		

		if(this.importData?.isImport) this.notifyReadiness();
	}

	setTimeUnit = (val) => WorkSpaceController.getNode(this.nodeId).data['timeUnit'] = val;
	setTriggerTime = (val) => WorkSpaceController.getNode(this.nodeId).data['time'] = val;
	setTargetPipeline = (val) => WorkSpaceController.getNode(this.nodeId).data['targetPipeline'] = val;

	onOutputConnection(){
		PipelineStepTrigger.handleOutputConnection(this);
		return { nodeCount: this.nodeCount.value };
	}

	/** @param { InputConnectionType<{}> } param0 */
	onInputConnection({ type, data }){
		if(type === DuckDBOutput.name) this.showOptions = true;
		this.leaderDataSource = data.datasetName;
		PipelineStepTrigger.handleInputConnection(this, data, type);
	}

	onConectionDelete = () => {
		if(this.showOptions !== false) this.showOptions = false;
	}

	addPipelineTrigger(){
		const curretDiagram = this.$parent.editor.export().drawflow.Home;
		const originalDiagram = JSON.parse(this.$parent.service.curImportedPipelineJSON).pipelineCode.content.Home;		
		const nodes = Object.keys(curretDiagram.data);
		for(const node of nodes){
			if(!(node in originalDiagram.data))
				originalDiagram.data[node] = curretDiagram.data[node];
		}
	}
	
}