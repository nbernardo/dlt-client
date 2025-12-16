import { NodeTypeInterface } from "../mixin/NodeTypeInterface.js";

export class NodeUtil {

    /**
     * 
     * @param { NodeTypeInterface } node 
     * @param {*} data 
     * @param {*} type 
     */
	static handleInputConnection(node, data, type ){
        
        if(Number(node?.nodeCount) === 0 && data.nodeCount != undefined){            
            node.nodeCount = 1;
            if('onOutputConnection' in node) node?.onOutputConnection();
            return;
        }

		if(type == 'Start') node.nodeCount = 1;
		else node.nodeCount = Number(data.nodeCount) + 1;
	}

    /** @param { NodeTypeInterface } node */
	static handleOutputConnection(node){        
        if(Number(node?.nodeCount.value) === 0 || node?.nodeCount.value === '')          
            node.nodeCount = 1;
	}

}