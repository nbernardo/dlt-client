import { BaseComponent } from "../../../../@still/component/super/BaseComponent.js";
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

}