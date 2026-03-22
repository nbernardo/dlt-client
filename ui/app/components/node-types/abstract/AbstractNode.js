import { BaseComponent } from "../../../../@still/component/super/BaseComponent.js";
import { WorkSpaceController } from "../../../controller/WorkSpaceController.js";
import { UserService } from "../../../services/UserService.js";
import { NodeTypeInterface } from "../mixin/NodeTypeInterface.js";

/** @implements { NodeTypeInterface } */
export class AbstractNode extends BaseComponent {

  nodeCount = '';

  notifyReadiness = () =>
    Components.emitAction(`nodeReady${this.cmpInternalId}`);

  onConectionDelete() {
    this.nodeCount = '';
  }

  /**
   * 
   * @param { NodeTypeInterface } node 
   * @param {*} data 
   * @param {*} type 
   */
  static handleInputConnection(node, data, type) {

    if (Number(node?.nodeCount) === 0 && data.nodeCount != undefined) {
      node.nodeCount = 1;
    } else {
      if (type == 'Start') node.nodeCount = 1;
      else {
        if ('nodeCount' in node && 'nodeCount' in (data || {}))
          node.nodeCount = Number(data.nodeCount) + 1;
      }
    }


    setTimeout(() => {
      try {
        //This'll make next node to update the nodeCount of itself 
        node.nextNode.onInputConnection({ data: node.onOutputConnection(), type: node?.nodeName });
      } catch (error) { }
    }, 100);

  }

  /** @param { NodeTypeInterface } node */
  static handleOutputConnection(node) {
    if (Number(node?.nodeCount.value) === 0 || node?.nodeCount.value === '')
      node.nodeCount = 1;
  }

  startReloadNode(){
    document
      .querySelector(`.${this.cmpInternalId}`)
      .querySelector('.reload-loc-node-btn').classList.add('reload-loc-node-btn-animace-rotation');
  }

  stopReloadNode(){
    document
      .querySelector(`.${this.cmpInternalId}`)
      .querySelector('.reload-loc-node-btn').classList.remove('reload-loc-node-btn-animace-rotation');
  }

  setNodeData = (field, value) => 
    WorkSpaceController.getNode(this.nodeId).data[field] = value;
  
  /**
   * This is specific to DLTCode kind of pipeline diagram node
   */
  async getCode(isDestination = false) {
		this.showTemplateList = true;
		WorkSpaceController.getNode(this.nodeId).data['dltCode'] = this.codeContent;
		WorkSpaceController.getNode(this.nodeId).data['namespace'] = await UserService.getNamespace();
    if(isDestination){
      const { configDetails, pipelineDestination } = this.parsePipelineDestinationCode(this.codeContent);
      WorkSpaceController.getNode(this.nodeId).data['configDetails'] = configDetails;
      WorkSpaceController.getNode(this.nodeId).data['pipelineDestination'] = pipelineDestination;
    }
	}

  /** @param {String} code */
  parsePipelineDestinationCode(code){
    const configDetails = code.match(/(config|credentials)\s*\=\s*\n*\t*\{[\s\S]*\}/i);
    let pipelineDestination = code.match(/destination[^(]*/);
    if(pipelineDestination){
      pipelineDestination = pipelineDestination[0].split('=')[1];
      // Handles the scenario where full dlt destination 
      // path is specified (e.g. dlt.destination.databricks)
      if(pipelineDestination.includes('.'))
        pipelineDestination = String(pipelineDestination).split('.').slice(-1)[0];
    }
    return { configDetails: configDetails && String(configDetails[0]), pipelineDestination };
  }

}