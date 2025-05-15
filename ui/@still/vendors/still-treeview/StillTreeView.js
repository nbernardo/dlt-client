import { ViewComponent } from "../../component/super/ViewComponent.js";


export class TreeNodeType { content; parentLbl; childs = []; parentData; }

export class StillTreeView extends ViewComponent {

	isPublic = true;

	dataSource;

	template = `<ul class="still-tree-view" id="@dynCmpGeneratedId">
					<tree-placeholder>
				</ul>`;

	/** @Prop @type { {} } */
	#treeElements;
	/** @Prop */
	#treeData = null;
	/** @Prop */
	#wasTreeLoaded = false;	
	/** @Prop */
	#topElementTemplate = null;
	
	showBullets = true;

	stAfterInit(){

		this.#treeElements = {};
		const self = this;
		let treeStructure = '';
		
		this.dataSource.onChange(treeMapping => {

			const treeData = Object.values(treeMapping);
			for(const node of treeData){
				const topNode = self.parseNode(node);
				treeStructure += `<li>${topNode}</li>`;
			}
			
			const container = document
				.getElementById(this.dynCmpGeneratedId);
			
			if(!this.showBullets.value){				
				container.style.setProperty('--child-circle','none');
				container.style.setProperty('--parent-circle','none');
			}

			container.innerHTML = container.innerHTML.replace('<tree-placeholder>',treeStructure)
		});

	}

	/** @param { { childs: [], nodeLabel, parentData } } param */
	parseNode(param = {}, returnValue = false){
		
		const self = this;

		const { childs, nodeLabel } = param;
		const details = document.createElement('details');
		details.setAttribute('open','')
		const summary = document.createElement('summary');
		const childsContainer = document.createElement('ul');
		let topContent = nodeLabel;

		if(this.#topElementTemplate){
			topContent = this.#topElementTemplate
				.replace('@replace',topContent)
				.replace('@data', param.parentData);
		}
		topContent = this.parseEvents(topContent);

		if(nodeLabel) summary.innerHTML = topContent;
		details.appendChild(summary);
		details.appendChild(childsContainer);
		
		for(const currNode of childs){
			const childElm = document.createElement('li');
			const content = document.createElement('span');
			content.innerHTML = currNode;
			childElm.appendChild(content);
			childsContainer.appendChild(childElm);

			if(currNode?.childs?.length)
				childElm.appendChild(self.parseNode(currNode, true));
		}

		//Return any intrmediate child node
		if(returnValue) return details;
		
		//Return the top most node
		return details.outerHTML;

	}

	addData(data){
		this.#treeData = data;
		return this;
	}

	loadTree(){
		if(!this.#wasTreeLoaded){
			if(this.#treeData) this.dataSource = this.#treeData;
			else this.dataSource = this.#treeElements;
			this.#wasTreeLoaded = true;
		}
	}

	/** @param { TreeNodeType } node */
	addElement(node){
		if(!(node.parentLbl in this.#treeElements)){
			this.#treeElements[node.parentLbl] = { 
				childs: [], nodeLabel: node.parentLbl, parentData: node.parentData 
			};
		}

		// In case any event (e.g. onclick) is being
		// passe parse will take care of it
		const parsedContent = this.parseEvents(node.content);
		this.#treeElements[node.parentLbl].childs.push(parsedContent)
		return this;
	}

	clearTreeData(){
		this.#treeElements = {};
	}

	/**
	 * @param { TreeNodeType } param 
	 */
	newElement({ content, parentLbl, childs }){
		if(!childs) childs = [];
		return { content, parentLbl, childs };
	}

	removeBullets(){
		this.defaultBullets = false;
		return this;
	}

	setTopElementTemplate(htmlString){
		this.#topElementTemplate = htmlString;
	}
	
}