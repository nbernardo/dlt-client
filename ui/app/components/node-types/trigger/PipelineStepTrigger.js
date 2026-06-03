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
	triggerValue;
	timeUnit;
	targetPipeline;

	/** @Prop */ aiGenerated;
	/** @Prop */ isImport;
	/** @Prop */ importData = null;
	/** @Prop */ leaderDataSource = null;
	/** @Prop */ showSettings = false;
	/** @Prop */ triggerOrder;
	/** @Prop */ settings = null;

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
		this.triggerAfterValues = Array.from({ length: 61 }, (_, label) => ({label}));
		this.pipelineList = (await PipelineService.getPipelinesShortList());

		this.triggerValue.onChange(val => this.setData('time', val));
		this.timeUnit.onChange(val => this.setData('timeUnit', val));
		this.targetPipeline.onChange(val => this.setData('targetPipeline', val));

		if(this.importData?.isImport) {			
			this.showSettings = true;
			this.notifyReadiness();
			const { time, timeUnit, targetPipeline } = this.importData;
			setTimeout(() => document.querySelector(`select[placeholder="Select the pipeline"]`).value = targetPipeline, 50);
			this.triggerValue = time, this.timeUnit = timeUnit;
		}
	}

	setData(field, val){ this.setNodeData(field, val); this.updateSettings(); }

	onOutputConnection(){
		PipelineStepTrigger.handleOutputConnection(this);
		return { nodeCount: this.nodeCount.value, order: Number(this.triggerOrder) + 1 };
	}

	/** @param { InputConnectionType<{}> } param0 */
	onInputConnection({ type, data }){
		if(type === DuckDBOutput.name || type === PipelineStepTrigger.name) {
			// If the source node is another trigger then the order will be set by it (source)
			this.triggerOrder = type === DuckDBOutput.name ? 1 : data.order;
			this.showSettings = true;
			PipelineService.storePipelineTriggers.push(this);
			this.setData('order', this.triggerOrder);
		}
		this.leaderDataSource = data.datasetName;
		PipelineStepTrigger.handleInputConnection(this, data, type);
	}

	onConectionDelete = () => {
		if(this.showSettings !== false) this.showSettings = false;
		PipelineService.storePipelineTriggers.splice(this.triggerOrder - 1, 1);
	}

	stOnUnload(){
		PipelineService.storePipelineTriggers.splice(this.triggerOrder - 1, 1);
	}

	updateSettings(){
		const { targetPipeline, triggerValue, timeUnit, triggerOrder } = this;
		this.settings = { ppline: targetPipeline.value, triggerValue: triggerValue.value, timeUnit: timeUnit.value, order: triggerOrder };
	}

	addPipelineTrigger(){
		const currentDiagram = this.$parent.editor.export().drawflow.Home;
		let originalDiagram = JSON.parse(this.$parent.service.curImportedPipelineJSON).pipelineCode;
		const pplineLbl = originalDiagram.pipeline_lbl;
		originalDiagram = originalDiagram.content.Home;
		// This is the actuall pipline script filename
		const activeGrid = this.$parent.activeGrid.value.toLowerCase().replace(/\s/g, '_');

		const nodes = Object.keys(currentDiagram.data);
		for(const node of nodes){
			if(!(node in originalDiagram.data))
				originalDiagram.data[node] = currentDiagram.data[node];
			else{
				originalDiagram.data[node].outputs = currentDiagram.data[node].outputs;
				originalDiagram.data[node].inputs = currentDiagram.data[node].inputs;
			}
		}
		const diagram = { Home: { data: originalDiagram.data } };
		const payload = { drawflow: diagram, activeGrid, pplineLbl, settings: PipelineService.storePipelineTriggers.map(trg => trg.settings) };

		PipelineService.addTrigger(payload)
	}
	
}