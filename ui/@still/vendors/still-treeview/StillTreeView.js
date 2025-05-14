import { ViewComponent } from "../../component/super/ViewComponent.js";


export class TreeNodeType { label = ''; nodes = []; }

export class StillTreeView extends ViewComponent {

	isPublic = true;

	dataSource;

	stAfterInit(){

		const self = this;
		const treeWrapper = `<ul class="tree">{{}}</ul>`;
		let treeStructure = '';
		
		this.dataSource.onChange(treeMapping => {

			const treeData = Object.values(treeMapping);
			for(const node of treeData){
				const topNode = self.parseNode(node);
				treeStructure += `<li>${topNode}</li>`;
			}
			
			document
				.getElementById(this.dynCmpGeneratedId)
				.innerHTML = treeWrapper.replace('{{}}',treeStructure)
		});

	}

	/** @param { { childs: [], nodeLabel } } param */
	parseNode(param = {}, returnValue = false){
		
		const self = this;

		const { childs, nodeLabel } = param;
		const details = document.createElement('details');
		details.setAttribute('open','')
		const summary = document.createElement('summary');
		const childsContainer = document.createElement('ul');
		
		if(nodeLabel) summary.innerText = nodeLabel;
		details.appendChild(summary);
		details.appendChild(childsContainer);

		const fields = Object.keys(childs[0]);
		console.log(`FIELDS ARE: `, fields);
		
		for(const currNode of childs){

			const childElm = document.createElement('li');
			const nodeText = document.createTextNode(currNode.table);
			childElm.appendChild(nodeText);
			childsContainer.appendChild(childElm);

			if(currNode?.childs?.length){
				childElm.appendChild(self.parseNode(currNode, true));
			}

		}

		//Return any intrmediate child node
		if(returnValue) return details;
		
		//Return the top most node
		return details.outerHTML;

	}

	generateTreeNode(lbl, childs){}

	template = `<div id="@dynCmpGeneratedId"><>`;

	static importAssets(){
		return {
			styles: ['tree-view.css']
		}
	}
	
}