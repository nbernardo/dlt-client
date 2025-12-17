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
        }else{
            if(type == 'Start') node.nodeCount = 1;
            else {
                if('nodeCount' in node && 'nodeCount' in (data || {}))
                    node.nodeCount = Number(data.nodeCount) + 1;
            }
        }
        
        try {
            //This'll make next node to update the nodeCount of itself 
            node.nextNode.onInputConnection({ data: node.onOutputConnection(), type: node?.nodeName});
        } catch (error) {}
        
	}

    /** @param { NodeTypeInterface } node */
	static handleOutputConnection(node){        
        if(Number(node?.nodeCount.value) === 0 || node?.nodeCount.value === '')          
            node.nodeCount = 1;
	}

}