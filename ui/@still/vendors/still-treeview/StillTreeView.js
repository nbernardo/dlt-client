import { ViewComponent } from "../../component/super/ViewComponent.js";


export class TreeNodeType { 
	//This are the properties
	content; childs = []; isTopLevel;

	addChild = (child) => this.childs.push(child);

	constructor({ content, isTopLevel }){ 
		this.content = content;
		this.isTopLevel = isTopLevel;
	}
}

export class StillTreeView extends ViewComponent {

	isPublic = true;

	dataSource;

	template = `<ul class="still-tree-view" id="@dynCmpGeneratedId">
					<tree-placeholder>
				</ul>`;

	/** @Prop @type { {} } */
	#treeNodes;
	/** @Prop */
	#treeData = null;
	/** @Prop */
	#wasTreeLoaded = false;	
	/** @Prop */
	#lastParent = null;
	/** @Prop */
	#nodeCounter = 0;

	
	showBullets = true;
	showSummary = true;

	stAfterInit(){

		this.#treeNodes = {};
		const self = this;
		let treeStructure = '';
		this.#nodeCounter = 0;
		
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

	/** @param { TreeNodeType } param */
	parseNode(param = {}, returnValue = false){
		
		const self = this;

		const { childs, content } = param;
		const details = document.createElement('details');
		details.setAttribute('open','')
		const summary = document.createElement('summary');
		const childsContainer = document.createElement('ul');
		let topContent = content;

		summary.innerHTML = topContent;
		details.appendChild(summary);
		details.appendChild(childsContainer);
		
		for(const currNode of childs){
			const childElm = document.createElement('li');
			
			const content = document.createElement('span');
			content.innerHTML = currNode.content;
			childElm.appendChild(content);
			
			childsContainer.appendChild(childElm);

			if(currNode?.childs?.length){
				childElm.appendChild(self.parseNode(currNode, true));
				childElm.removeChild(childElm.childNodes[0])				
			}
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

	renderTree(){
		if(!this.#wasTreeLoaded){
			if(this.#treeData) this.dataSource = this.#treeData;
			else this.dataSource = this.#treeNodes;
			this.#wasTreeLoaded = true;
		}
	}

	/** @param { TreeNodeType } node */
	addNode(node){
		node = new TreeNodeType(node);
		if(node.isTopLevel){
			if(!(++this.#nodeCounter in this.#treeNodes)){
				this.#treeNodes[this.#nodeCounter] = node;
				this.#lastParent = this.#treeNodes[this.#nodeCounter];
			}
		}

		return this.parseEvents(node);
	}

	/** 
	 * @param { TreeNodeType } parent 
	 * @param { TreeNodeType } child 
	 * */
	addChildsToNode(parent, child){
		parent.childs.push(child);
	}

	clearTreeData(){
		this.#treeNodes = {};
	}

	removeBullets(){
		this.defaultBullets = false;
		return this;
	}

	getTreeData(){
		if(this.#treeData) return this.#treeData;
		return this.#treeNodes;	
	}

	/** @returns { TreeNodeType } */
	getLastProcessSubtree(){
		return this.#lastParent;
	}
	
}