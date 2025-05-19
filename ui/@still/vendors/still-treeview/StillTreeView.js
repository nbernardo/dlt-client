import { ViewComponent } from "../../component/super/ViewComponent.js";


export class TreeNodeType { 
	//This are the properties
	content; childs = []; isTopLevel; id;

	addChild = (child) => this.childs.push(child);

	constructor({ content, childs, isTopLevel, id }){ 
		this.content = content;
		this.childs = childs;
		this.isTopLevel = isTopLevel;
		this.id = id;
	}
}

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
	#lastParent = null;

	
	showBullets = true;
	showSummary = true;

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

	loadTree(){
		if(!this.#wasTreeLoaded){
			if(this.#treeData) this.dataSource = this.#treeData;
			else this.dataSource = this.#treeElements;
			this.#wasTreeLoaded = true;
		}
	}

	/** @param { TreeNodeType } node */
	addElement(node){
		node = new TreeNodeType(node);
		if(node.isTopLevel){
			if(!(node.id in this.#treeElements)){
				this.#treeElements[node.id] = node;
				this.#lastParent = this.#treeElements[node.id];
			}
		}

		node.childs = [];
		node.content = this.parseEvents(node.content);
		return node;
	}

	/** 
	 * @param { TreeNodeType } parent 
	 * @param { TreeNodeType } child 
	 * */
	addChildsToElement(parent, child){
		parent.childs.push(child);
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

	getTreeData(){
		if(this.#treeData) return this.#treeData;
		return this.#treeElements;	
	}

	/** @returns { TreeNodeType } */
	getLastProcessSubtree(){
		return this.#lastParent;
	}
	
}