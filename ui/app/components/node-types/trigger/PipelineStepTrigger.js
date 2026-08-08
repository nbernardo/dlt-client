import { PipelineService } from "../../../services/PipelineService.js";
import { Workspace } from "../../workspace/Workspace.js";
import { AbstractNode } from "../abstract/AbstractNode.js";
import { DuckDBOutput } from "../DuckDBOutput.js";
import { NodeTypeInterface } from "../mixin/NodeTypeInterface.js";
import { InputConnectionType } from "../types/InputConnectionType.js";

const triggerStatus = { STATUS_PAUSE: 'PAUSED', STATUS_STOP: 'STOPED', STATUS_ACTIVE: 'ACTIVE' }

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
	buttonLabel = 'Link trigger';

	/** @Prop */ aiGenerated;
	/** @Prop */ isImport;
	/** @Prop */ importData = null;
	/** @Prop */ leaderDataSource = null;
	/** @Prop */ showSettings = false;
	/** @Prop */ triggerOrder;
	/** @Prop */ settings = null;
	/** @Prop */ update = true;
	/** @Prop */ container = true;

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
		this.container = document.querySelector(`.${this.cmpInternalId}`);
		this.showLoading = false;
		this.triggerAfterValues = Array.from({ length: 61 }, (_, label) => ({label}));
		this.pipelineList = (await PipelineService.getPipelinesShortList());

		this.triggerValue.onChange(val => this.setData('time', val));
		this.timeUnit.onChange(val => this.setData('timeUnit', val));
		this.targetPipeline.onChange(val => this.setData('targetPipeline', val));

		if(this.importData?.isImport) {
			if(String(triggerStatus.STATUS_ACTIVE).trim() === String(this.importData.status).trim()){
				this.assignUpdateFlag(false);
			}else{
				this.buttonLabel = 'Activate Link';
				this.assignUpdateFlag(true)
			}
			
			this.showSettings = true;
			this.notifyReadiness();
			const { time, timeUnit, targetPipeline, order } = this.importData;
			setTimeout(() => this.container.querySelector(`select[placeholder="Select the pipeline"]`).value = targetPipeline, 50);
			this.triggerValue = time, this.timeUnit = timeUnit, this.triggerOrder = order;
			PipelineService.storePipelineTriggers.push(this);
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
		return this.settings;
	}

	updateTrigger = async (status) => await this.addPipelineTrigger(triggerStatus[status]);

	async addPipelineTrigger(status){

		const currentDiagram = this.$parent.editor.export().drawflow.Home;
		let originalDiagram = JSON.parse(this.$parent.service.curImportedPipelineJSON).pipelineCode;
		const pplineLbl = originalDiagram.pipeline_lbl;
		originalDiagram = originalDiagram.content.Home;
		// This is the actuall pipline script filename
		const activeGrid = this.$parent.activeGrid.value.toLowerCase().replace(/\s/g, '_');

		const nodes = Object.keys(currentDiagram.data);
		for(const node of nodes){
			if(node in originalDiagram.data){
				if(originalDiagram.data[node].data.componentId == this.importData.componentId){
					originalDiagram.data[node].data.status = status || triggerStatus.STATUS_ACTIVE;
					originalDiagram.data[node].data.targetPipeline = this.targetPipeline.value;
					originalDiagram.data[node].data.time = this.triggerValue.value;
					originalDiagram.data[node].data.timeUnit = this.timeUnit.value;
				}
			}

			if(!(node in originalDiagram.data))
				originalDiagram.data[node] = currentDiagram.data[node];
			else{
				originalDiagram.data[node].outputs = currentDiagram.data[node].outputs;
				originalDiagram.data[node].inputs = currentDiagram.data[node].inputs;
			}
		}
		let diagram = { Home: { data: originalDiagram.data } };
		const payload = { drawflow: diagram, activeGrid, pplineLbl, settings: [this.updateSettings()] };

		if(status) {
			if(!(this.importData.status in Object.values(triggerStatus)))
				this.buttonLabel = 'Link trigger';
			payload.settings[0].status = status;
		}
		
		const result = await PipelineService.addTrigger(payload);

		if(result){
			if([triggerStatus.STATUS_ACTIVE, undefined].includes(status))
				this.update = false;
			else
				this.update = true;
		}
	}

	assignUpdateFlag(val){
		// Due to issues in hte framework assigning twice is needed
		this.update = val;
		this.update = val;
	}
	
}