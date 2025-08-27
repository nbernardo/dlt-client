import { ViewComponent } from "../../component/super/ViewComponent.js";
import { UUIDUtil } from "../../util/UUIDUtil.js";


export class TreeNodeType { 
	//This are the properties
	content; childs = []; isTopLevel;
	/** @param { TreeNodeType } child */
	addChild = (child) => this.childs.push(child);

	constructor({ content, isTopLevel }){ 
		this.content = content;
		this.isTopLevel = isTopLevel;
	}
}

export class StillTreeView extends ViewComponent {

	isPublic = true;

	dataSource;

	/** @Prop */
	treeContainerId = `_${UUIDUtil.newId()}`;

	/** @Prop @type { {} } */
	#treeNodes = {};
	/** @Prop */
	#treeData = null;
	/** @Prop */
	#wasTreeLoaded = false;	
	/** @Prop */
	#lastParent = null;
	/** @Prop */
	#nodeCounter = 0;

	/** @Prop */
	showBullets = true;
	/** @Prop */
	showRefresh = false;
	/** @Prop */
	tooltipText = 'Refresh the tree';
	/** @Prop */
	tooltipXPos = 200;
	/** @Prop */
	showLoader = true;
	

	template = `<ul class="still-tree-view" id="@dynCmpGeneratedId">
					<!-- THIS IS A REFRESH ICON -->
					<span 
						class="tree-refresh-container" 
						tooltip-x="@tooltipXPos"
						(showIf)="self.showRefresh"
						tooltip="@tooltipText">
						<svg
							(click)="onRefreshClick()" 
							width="15"
							id="Layer_1" data-name="Layer 1" 
							xmlns="http://www.w3.org/2000/svg" 
							viewBox="0 0 122.61 122.88">
							<title>update</title>
							<path d="M111.9,61.57a5.36,5.36,0,0,1,10.71,0A61.3,61.3,0,0,1,17.54,104.48v12.35a5.36,5.36,0,0,1-10.72,0V89.31A5.36,5.36,0,0,1,12.18,84H40a5.36,5.36,0,1,1,0,10.71H23a50.6,50.6,0,0,0,88.87-33.1ZM106.6,5.36a5.36,5.36,0,1,1,10.71,0V33.14A5.36,5.36,0,0,1,112,38.49H84.44a5.36,5.36,0,1,1,0-10.71H99A50.6,50.6,0,0,0,10.71,61.57,5.36,5.36,0,1,1,0,61.57,61.31,61.31,0,0,1,91.07,8,61.83,61.83,0,0,1,106.6,20.27V5.36Z"/>
						</svg>
					</span>
					<st-loader (showIf)="self.showLoader">
					<div class="tree-view-blank-space"></div>
				</ul>
				<style>
					.tree-refresh-container{
						margin-left: 15px;
						position: absolute;
						margin-top: -20px;
					}

					.tree-view-blank-space{ height: 20px; }
				</style>

				`;

	stAfterInit(){

		this.#treeNodes = {};
		const self = this;
		this.#nodeCounter = 0;
		
		this.dataSource.onChange(treeMapping => {
			const treeData = Object.values(treeMapping);
			self.parseAndPresentTreeData(treeData);
		});

		this.emit('load');

	}

	parseAndPresentTreeData(data){

		this.showLoader = true;
		let treeStructure = '';
		const treeData = Object.values(data);
		for(const node of treeData){
			const topNode = this.parseNode(node);
			treeStructure += `<li class="still-treeview-node">${topNode}</li>`;
		}
		
		this.stWhenReady(() => {
			const container = document
				.getElementById(this.dynCmpGeneratedId);

			if(!this.showBullets){				
				container.style.setProperty('--child-circle','none');
				container.style.setProperty('--parent-circle','none');
			}
			container.insertAdjacentHTML('beforeend',treeStructure);
			this.showLoader = false;		
		});
		
	}

	/** @param { TreeNodeType } param */
	parseNode(param = { childs: [] }, returnValue = false){
		
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
	/** @param {TreeNodeType} data  */
	addData(data){
		this.#treeData = data;
		return this;
	}

	renderTree(){
		let data = this.#treeData;
		if(!data) data = this.#treeNodes;
		
		if(!this.#wasTreeLoaded){
			this.parseAndPresentTreeData(data);
			this.#wasTreeLoaded = true;
		}else{
			//this.dataSource = data;
			const treeData = Object.values(data);
			this.parseAndPresentTreeData(treeData);
		}

	}

	/** 
	 * @param { TreeNodeType } node 
	 * @returns { TreeNodeType }
	 * */
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
		this.#treeData = null;
		this.dataSource = {};
		const treeContainer = document.getElementById(this.dynCmpGeneratedId);
		if(treeContainer){
			treeContainer.querySelectorAll('.still-treeview-node').forEach(elm => treeContainer.removeChild(elm));
		}
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

	//Signature to be implemented 
	// from parent component
	onRefreshClick(){}
	
}