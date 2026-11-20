import { STForm } from "../../../../@still/component/type/ComponentType.js";
import { Workspace } from "../../workspace/Workspace.js";
import { AbstractNode } from "../abstract/AbstractNode.js";
import { NodeTypeInterface } from "../mixin/NodeTypeInterface.js";
import { SourceMapper } from "../source-map/SourceMapper.js";
import { InputConnectionType } from "../types/InputConnectionType.js";


/** @implements { NodeTypeInterface } */
export class TimerNode extends AbstractNode {

	isPublic = false;

	/** This is strictly to reference the object in the diagram 
	 * @Prop */ nodeId;
	/** @Prop */ label = 'Timer';
	/** @Prop */ showLoading = true;
	/** @Prop */ inConnectors = 1;
	/** @Prop */ outConnectors = 1;
	/** @Prop @type { STForm } */ formRef;

	nodeCount = '';
	timeUnit;
	timerValues;
	selectedValue;

	/** @Prop */ aiGenerated;
	/** @Prop */ isImport;
	/** @Prop */ importData = null;
	/** @Prop */ showSettings = false;
	/** @Prop */ settings = null;
	/** @Prop */ update = true;
	/** @Prop */ container = true;
	/** @Prop */ source = null;

	/** @type { Workspace } */ $parent;

	async stOnRender(data){
		const { nodeId, aiGenerated } = data;
		this.nodeId = nodeId;
		this.aiGenerated = aiGenerated;
		this.importData = data;
	}

	async stAfterInit(){
		this.container = document.querySelector(`.${this.cmpInternalId}`);
		this.showLoading = false;
		this.timerValues = Array.from({ length: 61 }, (_, label) => ({label}));

		this.timeUnit.onChange(val => this.setData('timeUnit', val));
		this.selectedValue.onChange(val => this.setData('timeValue', val));

		if(this.importData?.isImport) {			
			this.showSettings = true;
			this.notifyReadiness();
			const { timeUnit } = this.importData;
			this.timeUnit = timeUnit;
		}
	}

	setData(field, val){ this.setNodeData(field, val); this.updateSettings(); }

	onOutputConnection(){
		// this.source will convey the connected object to the next object it's connecting to
		TimerNode.handleOutputConnection(this);
		return { nodeCount: this.nodeCount.value, source: this.source };
	}

	/** @param { InputConnectionType<{}> } param0 */
	onInputConnection({ type, data }){
		this.source = data.source;
		if(type === SourceMapper.name) this.showSettings = true;
		TimerNode.handleInputConnection(this, data, type);
	}

	onConectionDelete = () => {
		if(this.showSettings !== false) this.showSettings = false;
	}

	updateSettings(){
		const { timeUnit } = this;
		this.settings = { timeUnit: timeUnit.value };
		return this.settings;
	}
	
}