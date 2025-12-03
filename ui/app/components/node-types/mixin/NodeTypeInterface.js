/**
 * @interface
 */
export class NodeTypeInterface {

	nodeId;
	inConnectors;
	outConnectors;
	formRef;
	componentId;

	/** Implemented at each node as needed, but the call is automatically
	 *  through WorkspaceController when specific node recieces connection */
	onInputConnection(sourceNode, data){}

	/** Implemented at each node as needed, but the call is automatically
	 * through the WorkspaceController when connecting to specific node */
	onOutputConnection(){}

	/** This method is being implemented for every nodeType that needs to hold the auto-call
	 *  of onOutputConnection() which notifies the node receiveng connection through
	 *  onInputConnection. Needs to be called according to each node readiness logic
	 *  The implemente code will be Components.emitAction(`nodeReady${this.cmpInternalId}`) */
	notifyReadiness(){}

	/** @returns { boolean } */
	validate(){}

}