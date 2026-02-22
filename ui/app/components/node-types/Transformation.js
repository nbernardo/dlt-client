import { sleepForSec } from "../../../@still/component/manager/timer.js";
import { STForm } from "../../../@still/component/type/ComponentType.js";
import { Components } from "../../../@still/setup/components.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";
import { WorkSpaceController } from "../../controller/WorkSpaceController.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";
import { dataToTable } from "../../util/dataPresentationUtil.js";
import { Workspace } from "../workspace/Workspace.js";
import { AbstractNode } from "./abstract/AbstractNode.js";
import { Bucket } from "./Bucket.js";
import { NodeTypeInterface } from "./mixin/NodeTypeInterface.js";
import { SqlDBComponent } from "./SqlDBComponent.js";
import { TRANFORM_ROW_PREFIX, TransformRow } from "./transform/TransformRow.js";
import { InputConnectionType } from "./types/InputConnectionType.js";
import { DatabaseTransformation, TransformExecution } from "./util/tranformation.js";

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
	/** @Prop @type { SqlDBComponent|Bucket } */ sourceNode = null;
	/** @Prop */ dataSourceType = null;
	/** @Prop */ sqlConnectionName = null;
	/** @Prop */ aiGenerated;
	/** @Prop */ importFields;
	/** @Prop */ gettingTransformation = false;
	/** @Prop */ fileSource = null;

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

		let { tables, sourceNode } = data;
		Transformation.handleInputConnection(this, data, type);
		// This is the bucket component itself
		this.sourceNode = sourceNode;

		this.dataSourceType = null, this.sqlConnectionName = null, this.fileSource = null;
		if ([Bucket.name, SqlDBComponent.name].includes(type)) {
			
			this.databaseList = tables;
			[...this.fieldRows].forEach(([_, row]) => {
				row.dataSourceList = tables;
				row.databaseFields = this.sourceNode.tablesFieldsMap;
			});

			// In case the SQL Database changes, it proliferates downstream 
			// thereby updating the Transformation and different added transformations
			if(SqlDBComponent.name == type){

				this.dataSourceType = 'SQL', this.sqlConnectionName = this.sourceNode.selectedSecret.value;
				this.sourceNode.selectedSecretTableList.onChange(value => {
					value = value.map(table => ({ name: table, file: table }))
					this.databaseList = value;
					[...this.fieldRows].forEach(([_, row]) => {
						row.dataSourceList = value
						row.databaseFields = this.sourceNode.tablesFieldsMap;
					});
				});
			}else{
				this.fileSource = (this.sourceNode.selectedFilePattern.value || '').replace('*','');
				this.dataSourceType = 'BUCKET';
				this.sourceNode.selectedFilePattern.onChange(value => {
					this.fileSource = (value || '').replace('*','');
				});
			}
		}
	}

	/** @returns { InputConnectionType } */
	onOutputConnection(){
		Transformation.handleOutputConnection(this);
		//This will emit the source node as Bucket or SQLDB to the node it'll connect
		return { sourceNode: this.sourceNode, nodeCount: this.nodeCount.value };
	}
	
	async addNewField(data = null, inTheLoop = false, isNewField = false) {

		const obj = this;

		if(this.isImport === true && inTheLoop === false && this.confirmModification === false && this.$parent.isAnyDiagramActive){
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
			const initialData = { dataSources, rowId, importFields: data, tablesFieldsMap: obj.sourceNode?.tablesFieldsMap, isImport: obj.isImport, isNewField };
			
			// Create a new instance of TransformRow component
			const { component, template } = await Components.new(TransformRow, initialData, parentId);

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

	parseTransformationCode(toPreview = false) {
		let finalCode = "", rowsConfig = [];
		const data = WorkSpaceController.getNode(this.nodeId).data;
		data['dataSourceType'] = null;

		// if(this.dataSourceType != 'SQL' && this.sourceNode.getName() !== SqlDBComponent.name){
		// 	const util = DatabaseSourceTransform; //NonDatabaseSourceTransform
		// 	finalCode = util.sourceTransformation(this.transformPieces, rowsConfig);
		// }
		// else{
		const util = DatabaseTransformation;
		util.sourceTransformation(this.transformPieces, rowsConfig);
		finalCode = DatabaseTransformation.transformations;
		const transformTable = Object.entries(finalCode);
		if(toPreview === false){
			for(let [table, transforms] of transformTable){
				const totalTransform = transforms.length;
				for(let x = 0; x < totalTransform; x++){
					const transform = transforms[x] || '';
					const isObject = Object.prototype.toString.call(transform) === '[object Object]';
					if(isObject)
						if('aggreg' in transform) continue;
					
					const isTransformation2 = 
						transform.includes('df.unique(subset=[') || transform.includes('df.drop([') || transform.includes('df.filter(')
					if(isTransformation2)
						finalCode[table].splice(x,1);
				}
			}
		}
		data['dataSourceType'] = 'SQL';
		//}
		// Other code handles transformation such as deduplication, column drop, etc.
		const otherCode = DatabaseTransformation.otherTransformations;
		//console.log(`Transformation in: `, DatabaseTransformation.transformations);
		data['code'] = finalCode, data['code2'] = otherCode, data['rows'] = rowsConfig;
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

	async getTransformationPreview(){
		DatabaseTransformation.transformTypeMap = {};
		this.gettingTransformation = true;
		TransformExecution.validationErrDisplayReset(this.cmpInternalId);
		let transformations = this.parseTransformationCode(true), script = '', finalScript = '', result = '';

		if(transformations == '') 
			return setTimeout(() => this.gettingTransformation = false, 500);
		if(Object.keys(transformations) == 0){
			result = '<div style="width: 100%; text-align: center; color: green;">No relevant transformation to run</div>';
			TransformExecution.transformPreviewShow(this.cmpInternalId, result);
			return setTimeout(() => this.gettingTransformation = false, 500);
		}

		const tablesSet = Object.entries(transformations)
		const newFieldRE = /alias\(\'([A-Z]{1,}[A-Z1-9]{0,})(_New){0,}\'\)/ig

		const connectionName = this.sourceNode.selectedSecret?.value;
		const dbEngine = this.sourceNode?.selectedDbEngine?.value;
		const transformRowMapping = {};
		
		if(tablesSet.length > 0) finalScript = 'results, lfquery = [], None\n';
		let count = 1, cols, prevAg, transformCount;

		for(const [tableName, transformations] of tablesSet){

			script = 'try:\n\t', cols = '*';
			[script, cols] = Transformation.setPolarsDataFrameSource(script, tableName, cols, dbEngine);
			
			transformRowMapping["lfquery.with_columns("+transformations[0]+")"] = count;
			let totalTransform = transformations.length;
			transformCount = 0, prevAg = null;

			for(let transformation of transformations){
				const isObject = (Object.prototype.toString.call(transformation) === '[object Object]');
				
				if(isObject){
					if('aggreg' in transformation){
						[script, prevAg] = Transformation.parseAggregation(script, transformation, prevAg);
						transformCount++;
						if(transformCount == totalTransform)
							[script, count, finalScript] = Transformation.endTransfPreviewScope(script, count, tableName, finalScript);
						continue;
					}
				}

				if(transformCount < totalTransform && prevAg !== null){
					[script, count, finalScript] = Transformation.endTransfPreviewScope(script, count, tableName, finalScript);
					// Resets the script for the next transformation
					script = 'try:\n\t', cols = '*';
					[script, cols] = Transformation.setPolarsDataFrameSource(script, tableName, cols, dbEngine);
					script += `lf = lfquery.with_columns(${transformations[transformCount]})\n\t`;
					
					transformCount++
				}else{
					script += `lf = lfquery.with_columns(${transformations[0]})\n\t`;
				}
				
				if(transformation.trim() == '') continue;
				const { type, isNewField } = DatabaseTransformation.transformTypeMap[`${tableName}-${transformation}`];
				const isDedupTransform = type === 'DEDUP', isDropTransform = type === 'DROP', isFilterTransform = type === 'FILTER';

				if(isNewField) transformation = transformation.replace(".alias('",".alias('+");

				else if(isDedupTransform || isDropTransform || isFilterTransform) {
					const transformType = { isDedupTransform, isDropTransform, isFilterTransform };

					[ script, count, transformCount, finalScript ] = Transformation.parseScript({
						script, tableName, transformType, count, transformCount, 
						finalScript, transformation, transformations, totalTransform
					});

					totalTransform--;
					continue;
				}

				const isCalcTransform = type === 'CALCULATE',  isSplitTransform = type === 'SPLIT';
				const otherValidTransform = isCalcTransform || isSplitTransform;

				if(!transformation.startsWith('pl.when(') && !otherValidTransform) {
					count++;
					continue;
				}

				[ script, count, transformCount, finalScript ] = Transformation.parseFilter(
					{ script, count, tableName, otherValidTransform, isSplitTransform, isNewField, finalScript,
						newFieldRE, isCalcTransform, transformation, transformations, transformCount, totalTransform
					}
				);

				totalTransform--;
			}
			finalScript += '\n\n';
		}
		DatabaseTransformation.transformTypeMap = {};
		//In case there is not relevant transformation (e.g. change case to upper or lower)
		if(finalScript.trim() === 'results, lfquery = [], None'){
			this.gettingTransformation = false;
			result = '<div style="width: 100%; text-align: center; color: green;">No relevant transformation to run</div>';
			return TransformExecution.transformPreviewShow(this.cmpInternalId, result);
		}
		
		finalScript = finalScript.replaceAll(".alias('+",".alias('").replace("pl.col('+","pl.col('");
		finalScript = finalScript.replaceAll('"(pl.','(pl.');

		const sourceType = this.dataSourceType;
		const previewResult = await WorkspaceService.getTransformationPreview(
			connectionName, finalScript, dbEngine, sourceType, this.fileSource
		);
		if(previewResult === null) return;

		this.gettingTransformation = false;
		if(previewResult.code === '\n') return;
		if('error' in previewResult || previewResult.code){
			const affectedRow = Transformation.parseTransformException(previewResult, transformRowMapping, this.fieldRows);
			return affectedRow.displayTransformationError(previewResult.msg);
		}

		if(Array.isArray(previewResult) && previewResult.length > 0){
			for(const previewSet of previewResult){
				const { data, columns, table } = previewSet;
				result += dataToTable({ data, columns, cssClass: 'transform-preview-datatable' }, table, true);
				result += '<br>';
			}
		}else
			result = '<div style="width: 100%; text-align: center; color: red;">One or more transformations are invalid</div>';
		TransformExecution.transformPreviewShow(this.cmpInternalId, result);
	}

	resetTransformation(){
		this.gettingTransformation = false;
		[...this.fieldRows].forEach(([_, /** @type { Transformation } */row], idx) => {
			const /** @type { TransformRow } */ curRow = row;
			if(idx === 0) {
				curRow.selectedSource = '';
				curRow.selectedType = '';
				if(this.sourceNode) this.sourceNode.onOutputConnection();
			}
			else curRow.removeMe();
		});
		this.gettingTransformation = false;
	}

	static endTransfPreviewScope(script, count, tableName, finalScript){
		// closed the aggregator scope, which is opened within the parseAggregation
		script += '\n\t)\n\n\t';
		[script, count] = Transformation.parseTransformResult(script, count, tableName, false);
		finalScript += script;

		return [script, count, finalScript]
	}

	static setPolarsDataFrameSource(script, tableName, cols, dbEngine){
		if(dbEngine === 'mssql'){
			script += `table = '${tableName}'\n\t`;
			script += `schema_name, table_name = table.split('.')\n\t`;
			script += `columns = inspector.get_columns(table_name, schema=schema_name)\n\t`;
			script += `parsed_columns = column_type_conversion(columns, engine.connect(), table_name, schema_name)\n\t`;
			cols = '{parsed_columns}';
		}
			
		if(this.fileSource != null){
			let readType = 'scan_csv';
			if(tableName.endsWith('.parquet')) readType = 'scan_parquet';
			if(tableName.endsWith('.jsonl')) readType = 'scan_ndjson'

			script += `lfquery = pl.${readType}(f'%pathToFile%/${tableName.replace('*','')}')\n\t`;
		}else{
			script += `lfquery = pl.read_database(f'SELECT ${cols} FROM ${tableName}', engine).lazy()\n\t`;
		}
		return [ script, cols ]
	}

	static parseAggregation(script, transformation, prevAggreg){
		// Bellow if statement starts the aggregator scope, which is closed outside this function
		if(transformation.field !== prevAggreg)
			script += `lf = lfquery.group_by('${transformation.field}').agg(`;
		
		script += `\n\t\t\t${transformation.aggreg},`;
		return [script, transformation.field];
	}

	static parseScript({
		script, tableName, transformType, count, transformCount, finalScript,
		transformation, transformations, totalTransform
	}){

		script = script.replace(transformation,'pl.all()');

		if(transformType.isDedupTransform)
			script += transformation.replace('df.unique(subset=','lf = lf.unique(subset=')+'\n\t';

		if(transformType.isDropTransform)
			script += transformation.replace('df.drop([','lf = lf.drop([')+'\n\t';
					
		if(transformType.isFilterTransform)
			script += transformation.replace('df.filter(','lf = lf.filter(')+'\n\t';

		[script, count] = Transformation.parseTransformResult(script, count, tableName, transformType.isDropTransform);

		finalScript += script;
		if(totalTransform > 1){
			script = `\n\ntry:\n\t`;
			script += `lf = lfquery.with_columns(${transformations[++transformCount]})\n\t`;
			script = script
					.replace(transformation+',','')
					.replace(transformation,'')
		}

		return [ script, count, transformCount, finalScript ]

	}

	static parseTransformResult(script, count, tableName, isDropTransform){
		script += `result = lf.${isDropTransform ? 'limit(2)' : 'limit(20)'}.collect()\n\t`;
		script += `results.append({ 'columns': result.columns, 'data': result.rows(), 'table': '${tableName}' })\n`;
		script += `except Exception as err:\n\t`;
		script += `print(f'Error #${count}#: {str(err)}')\n\t`;
		script += `raise Exception(f'Error #${count++}#: {str(err)}')\n\n`;
		return [script, count];
	}

	static parseFilter(
		{ script, count, tableName, otherValidTransform, isSplitTransform, isNewField, finalScript,
			newFieldRE, isCalcTransform, transformation, transformations, transformCount, totalTransform
		 }
	){
		let filterInstruction = '';
		if(!otherValidTransform){
			filterInstruction = transformation.split('pl.when(')[1];
			filterInstruction = filterInstruction.split(').then')[0];
		}else{
			if(isSplitTransform)
				filterInstruction = `${transformation.split('.str')[0]}.is_not_null()`;
			else if(isCalcTransform)
				filterInstruction = `pl.col${transformation.split('alias')[1]}.is_not_null()`;
		}

		script += `result = (lf.filter(${filterInstruction}).limit(5).collect())\n\t`;
		script += `results.append({ 'columns': result.columns, 'data': result.rows(), 'table': '${tableName}' })\n`;
		script += `except Exception as err:\n\t`;
		script += `print(f'Error #${count}#: {str(err)}')\n\t`;
		script += `raise Exception(f'Error #${count++}#: {str(err)}')`;
		//In case there is any deduplicate transformation in place
		script = script.replace(/\,{0,1}(\n|\t|\n\t){0,}df\.unique\(subset\=\[[A-Z0-9\']{0,}\]\)\,{0,1}(\n|\t|\n\t){0,}/ig, '');
				
		if(!isNewField)
			script = script.replace(newFieldRE, (mt, $1) =>  mt.replace(`${$1}`,`${$1}_New`));
				
		finalScript += script;

		script = '';
		if(totalTransform > 1) {
			//Reinstate things for next transformation
			script = '\n\ntry:\n\t';
			script += `lf = lfquery.with_columns(${transformations[++transformCount]})\n\t`;
		}
		return [ script, count, transformCount, finalScript ]
	}

	/** @returns { TransformRow } */
	static parseTransformException(previewResult, transformRowMapping, fieldRows){
		let transfomRowIndex;
		if(previewResult.msg.search(/Error\s{1}\#(\d)\#/) >= 0) 
			transfomRowIndex = Number(previewResult.msg.split('#')[1]) - 1;
		else{
			let codeRow = previewResult.code.replace('\tlf = ','');
			codeRow = codeRow.replace(newFieldRE, (mt, $1) =>  mt.replace(`${$1}_New`,`${$1}`));
			if(codeRow.endsWith('\n')) codeRow = codeRow.slice(0, codeRow.length - 1);
			transfomRowIndex = Number(transformRowMapping[codeRow]) - 1;
		}

		return [...fieldRows][transfomRowIndex][1];
	}
}