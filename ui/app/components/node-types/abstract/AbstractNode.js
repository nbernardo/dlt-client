import { BaseComponent } from "../../../../@still/component/super/BaseComponent.js";
import { NodeTypeInterface } from "../mixin/NodeTypeInterface.js";

/** @implements { NodeTypeInterface } */
export class AbstractNode extends BaseComponent {

    nodeCount = '';

    notifyReadiness = () => 
		Components.emitAction(`nodeReady${this.cmpInternalId}`);

    onConectionDelete(){
		this.nodeCount = '';
	}

}