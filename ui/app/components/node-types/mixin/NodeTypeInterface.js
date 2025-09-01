/**
 * @interface
 */
export class NodeTypeInterface {

	inConnectors;
	outConnectors;
	formRef;

	onInputConnection(sourceNode, data){}
	onOutputConnection(){}

	/** @returns { boolean } */
	validate(){}

}