import { sleepForSec } from "../../../@still/component/manager/timer.js";
import { STForm } from "../../../@still/component/type/ComponentType.js";
import { Components } from "../../../@still/setup/components.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";
import { WorkSpaceController } from "../../controller/WorkSpaceController.js";
import { Workspace } from "../workspace/Workspace.js";
import { AbstractNode } from "./abstract/AbstractNode.js";
import { Bucket } from "./Bucket.js";
import { NodeTypeInterface } from "./mixin/NodeTypeInterface.js";
import { SqlDBComponent } from "./SqlDBComponent.js";
import { TRANFORM_ROW_PREFIX, TransformRow } from "./transform/TransformRow.js";
import { InputConnectionType } from "./types/InputConnectionType.js";
import { NodeUtil } from "./util/nodeUtil.js";
import { DatabaseTransformation, NonDatabaseSourceTransform } from "./util/tranformation.js";

/** @implements { NodeTypeInterface } */
export class Transformation extends AbstractNode {

	isPublic = true;

	/** @Prop */ inConnectors = 1;
	/** @Prop */ outConnectors = 1;
	/** @Prop */ remainTransformRows = [];
	/** @Prop @type { STForm } */ formRef;
	/** @Prop */ uniqueId = '_' + UUIDUtil.newId();
	/** @Prop */ showLoading = false;
	/** @Prop */ confirmModification = false;
	/** @Prop */ sourceNode = null;
	/** @Prop */ dataSourceType = null;
	/** @Prop */ aiGenerated;
	/** @Prop */ importFields;

	/** This will hold all applied transformations
	 * @Prop @type { Map<TransformRow> } */
	transformPieces = new Map();

	databaseList = [{ name: '' }];
	nodeCount = '';

	/** This will hold all added transform
	 * @Prop @type { Map<TransformRow>|Array<TransformRow> } */
	fieldRows = new Map();

	/** This will only hold the row when importive/viewing 
	 *  previously created pipeline which has transformation
	 *  as one of its steps, and it's filled automatically by 
	 * the the WorkspaceController at processImportingNodes
	 * @Prop @type { Array } */
	rows = null;


	/** @Prop */ nodeId;
	/** @Prop */ isImport;

	/** @type { Workspace } */ $parent;

	stOnRender(data) {
		const { 
			nodeId, isImport, aiGenerated, row, rows, 
			numberOfRows, numberOfTransformations, rowCount, rowsCount 
		} = data;
		this.nodeId = nodeId;
		this.isImport = isImport;
		if (isImport === true) this.showLoading = true;
		this.aiGenerated = aiGenerated
		
		this.importFields = { row, rows, numberOfRows, nRows: numberOfTransformations, rowCount, rowsCount };
	}

	async stAfterInit() {

		if (this.isImport && this.rows !== null) {			
			this.databaseList = this.databaseList.value.map(itm => ({ ...itm, name: itm.name.replace('*','') }));
			for (const rowConfig of this.rows)
				await this.addNewField(rowConfig, true);
			await sleepForSec(500);
			this.showLoading = false;
			return
		}

		if(this.aiGenerated){

			let totalRows = this.importFields.numberOfRows;
			if(this.importFields.row) totalRows = this.importFields.row;
			if(this.importFields.rows) totalRows = this.importFields.rows;
			if(this.importFields.nRows) totalRows = this.importFields.nRows;
			if(this.importFields.rowCount) totalRows = this.importFields.rowCount;
			if(this.importFields.rowsCount) totalRows = this.importFields.rowsCount;

			if(!Number.isNaN(totalRows)){
				const length = Number(totalRows) - 1;
				Array.from({ length }, (_, v) => v).forEach(async () => await this.addNewField());				
			}
		}

		if (this.showLoading === true) this.showLoading = true;
		await this.addNewField();

	}

	/** @param { InputConnectionType<SqlDBComponent|Bucket> } */
	onInputConnection({ data, type }) {

		const { tables, sourceNode } = data;
		NodeUtil.handleInputConnection(this, data, type);
		
		this.dataSourceType = null;
		if ([Bucket.name, SqlDBComponent.name].includes(type)) {

			// This is the bucket component itself
			this.sourceNode = sourceNode;
			
			this.databaseList = tables;
			[...this.fieldRows].forEach(([_, row]) => {
				row.dataSourceList = tables;
				row.databaseFields = this.sourceNode.tablesFieldsMap;
			});

			// In case the SQL Database changes, it proliferates downstream 
			// thereby updating the Transformation and different added transformations
			if(SqlDBComponent.name == type){
				
				this.dataSourceType = 'SQL';
				this.sourceNode.selectedSecretTableList.onChange(value => {
					value = value.map(table => ({ name: table, file: table }))
					this.databaseList = value;
					[...this.fieldRows].forEach(([_, row]) => {
						row.dataSourceList = value
						row.databaseFields = this.sourceNode.tablesFieldsMap;
					});
				});
			}
		}
	}

	/** @returns { InputConnectionType } */
	onOutputConnection(){
		NodeUtil.handleOutputConnection(this);
		//This will emit the source node as Bucket or SQLDB to the node it'll connect
		return { sourceNode: this.sourceNode, nodeCount: this.nodeCount.value };
	}

	async addNewField(data = null, inTheLoop = false) {

		const obj = this;

		if(this.isImport === true && inTheLoop === false && this.confirmModification === false){
			this.confirmActionDialog(handleAddField);
		}else handleAddField();
		
		async function handleAddField() {
			let dataSources = obj.databaseList.value;
			
			if(obj.$parent.controller.importingPipelineSourceDetails !== null && obj.isImport){
				dataSources = obj.$parent.controller.importingPipelineSourceDetails.tables;
				dataSources = Object.keys(dataSources).map(tableName => ({ 'name': tableName }));
			}

			const parentId = obj.cmpInternalId;
			const rowId = TRANFORM_ROW_PREFIX + '' + UUIDUtil.newId();
			const initialData = { dataSources, rowId, importFields: data, tablesFieldsMap: obj.sourceNode?.tablesFieldsMap, isImport: obj.isImport };
			
			// Create a new instance of TransformRow component
			const { component, template } = await Components.new('TransformRow', initialData, parentId);

			// Add component to the DOM tree as part of transformation
			document.querySelector('.transform-container-' + obj.uniqueId).insertAdjacentHTML('beforeend', template);
			obj.fieldRows.set(rowId, component);
		}

	}

	removeField(rowId) {
		this.transformPieces.delete(rowId);
		this.fieldRows.delete(rowId);
		document.getElementById(rowId).remove();
	}

	parseTransformationCode() {
		let finalCode = "", rowsConfig = [];
		const data = WorkSpaceController.getNode(this.nodeId).data;
		data['dataSourceType'] = null;

		if(this.dataSourceType != 'SQL'){
			const util = NonDatabaseSourceTransform;
			finalCode = util.sourceTransformation(this.transformPieces, rowsConfig);
		}
		else{
			const util = DatabaseTransformation;
			util.sourceTransformation(this.transformPieces, rowsConfig);
			finalCode = DatabaseTransformation.transformations;
			data['dataSourceType'] = 'SQL';
		}
		console.log(`Transformation in: `, DatabaseTransformation.transformations);
		data['code'] = finalCode;
		data['rows'] = rowsConfig;

		return finalCode;

	}

	/** @param { Function } confirmEvent */
	confirmActionDialog(confirmEvent) {
		const message = `Removing/Adding a new transformation rule will override the existing data structure, and create new version of the pipeline. <br><br>Do you whish to proceed?`;
		this.$parent.controller.showDialog(message, {
			onConfirm: async () => {
				this.confirmModification = true;
				await confirmEvent(this);
			}
		});
	}

}
