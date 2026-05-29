import { STForm } from "../../../@still/component/type/ComponentType.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";
import { WorkSpaceController } from "../../controller/WorkSpaceController.js";
import { UserService } from "../../services/UserService.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";
import { InputDropdown } from "../../util/InputDropdownUtil.js";
import { AbstractNode } from "./abstract/AbstractNode.js";
import { NodeTypeInterface } from "./mixin/NodeTypeInterface.js";
import { InputConnectionType } from "./types/InputConnectionType.js";
import { databaseEnginesList, databaseIcons } from "./util/databaseUtil.js";
import { addSQLComponentTableField } from "./util/formUtil.js";

/** @implements { NodeTypeInterface } */
export class SqlDBComponent extends AbstractNode {

	isPublic = true;

	label = 'Source Database';
	databaseEngines = databaseEnginesList;

	/** @Prop */ inConnectors = 1;
	/** @Prop */ outConnectors = 1;
	/** @Prop */ nodeId;
	/** @Prop */ dbInputCounter = 1;
	/** @Prop @type { STForm } */ formRef;
	/** @Prop */ isOldUI;
	/** @Prop @type { TableAndPKType } */ dynamicFields;
	/** @Prop */ tablesFieldsMap;
	/** @Prop */ dbIcon = databaseIcons.generic;
	/** @Prop */ selectedTablesName = {};
	/** @Prop */ removeAddedTableCallbacks = [];

	selectedSecretTableList = [];
	selectedTableList = [];
	database = 'Not selected';
	tableName;
	selectedDbEngine = 'Not selected';
	selectedDbEngineDescription = 'Not selected';
	selectedSecret;
	primaryKey;
	incrementCol;
	secretList = [];
	hostName = 'None';
	nodeCount = '';

	/** @Prop */ isImport = false;
	/** @Prop */ formWrapClass = '_'+UUIDUtil.newId();
	/** @Prop @type { HTMLElement } */ container;

	/** @Prop @type { STForm } */ anotherForm;
	/** @Prop */ showLoading = false;
	/** @Prop */ secretedSecretTrace = null;
	/** @Prop */ aiGenerated = null;
	
	// tables and primaryKeys hold all tables name when importing/reading
	// An existing pipeline by calling the API
	/** @Prop @type { Map } */ tables;
	/** @Prop @type { Map } */ primaryKeys;
	/** @Prop @type { Map } */ incrementCols;
	/** @Prop */ importFields;
	/** @Prop */ showIncrementalByFields;

	/**
	 * @Inject @Path services/
	 * @type { WorkSpaceController } */
	wSpaceController;

	/**
	 * The id will be passed when instantiating SqlDBComponent dinamically
	 * through the Component.new(type, param) where for para nodeId 
	 * will be passed
	 * */
	stOnRender(data){		
		const { 
			nodeId, isImport, tables, primaryKeys, database, dbengine, isIncremental,
			connectionName, aiGenerated, asTemplate, fromPlan, incrementCols 
		} = data;		
		this.aiGenerated = aiGenerated;
		this.nodeId = nodeId;
		this.isImport = isImport;
		this.tables = tables;
		this.primaryKeys = primaryKeys;	
		this.incrementCols = incrementCols;	
		this.importFields = { database, dbengine, connectionName, asTemplate, changeCount: 0, fromPlan, isIncremental };
		if(data?.host) this.importFields.host = data.host;
	}

	async stAfterInit(){
		await this.getDBSecrets();
		this.isOldUI = this.templateUrl?.includes('SqlDBComponent_old.html');
		this.selectedSecretTableList = [], this.selectedTablesName = {};
		this.container = document.querySelector(`.${this.cmpInternalId}`);

		this.dynamicFields = new TableAndPKType();
		this.setupOnChangeListen();

		if(this.isImport === true) this.handleImportAssignement();
		if(this.aiGenerated === true) this.handleAiGenerated();
		
		const htmlTableInputSelector = 'input[data-id="firstTable"]', 
			  htmlPkInputSelector = 'input[data-id="firstPK"]',
			  htmlIncrementColInputSelector = 'input[data-id="firstIncrementCol"]';

		if(!this.isOldUI) this.handleTableFieldsDropdown(htmlTableInputSelector, htmlPkInputSelector, undefined, undefined, htmlIncrementColInputSelector);
		
		if(this.importFields.isIncremental === true) 
			document.querySelector(`.${this.cmpInternalId}`).parentElement.parentElement.style.width = '408px';

	}

	handleImportAssignement(){
		// At this point the WorkSpaceController was loaded by WorkSpace component
		// hance no this.wSpaceController.on('load') subscrtiption is needed
		this.wSpaceController.disableNodeFormInputs(this.formWrapClass);
		this.notifyReadiness();
		// This flac if for this new scenario where the pipeline is generated from the pipeline plan
		const isItPlanned = this.importFields.fromPlan;

		const disable = this.wSpaceController.shouldDisableNodeFormInputs;
		const allTables = isItPlanned ? Object.keys(this.tables).map(this.extractTableName) : Object.values(this.tables);
		const allKeys = Object.values(this.primaryKeys);
		const allIncrementCols = Object.values(this.incrementCols);
		const data = WorkSpaceController.getNode(this.nodeId).data;

		// Assign the first table
		this.tableName = isItPlanned ? allTables[0] : this.tables['tableName'];
		this.primaryKey = allKeys[0];
		this.incrementCol = allIncrementCols[0];
		this.selectedSecret = this.importFields.connectionName;

		// Assign remaining tables if more than one in the pipeline
		allTables.slice(1).forEach((tblName, idx) => this.newTableField(idx + 2, tblName, disable, allKeys[idx+1], allIncrementCols[idx+1]));
		this.dbInputCounter = allTables.length, this.hostName = this.importFields.host || 'None'
		this.selectedDbEngine = this.importFields.dbengine;
		this.setDBIcon(this.selectedDbEngine);
		document.querySelector('.add-table-buttons').disabled = this.wSpaceController.shouldDisableNodeFormInputs;
		data['database'] = this.database.value, data['dbengine'] = this.selectedDbEngine.value, data['host'] = this.hostName.value;
		
		this.showHideIncrementByField(this.importFields.isIncremental, true);
	}

	extractTableName = (tblPath) => {
		const path = tblPath.split('.');
		return path.length > 1 ? path.slice(1).join('.') : tblPath;
	}

	handleAiGenerated = () => this.selectedSecret = this.importFields.connectionName || '';

	handleTableFieldsDropdown(tableSelecter, pkSelecter, tableFieldName, pkFieldName, incrementCol){

		const pkField = InputDropdown.new({ 
			inputSelector: pkSelecter, dataSource: this.selectedTableList.value, boundComponent: this, componentFieldName: pkFieldName
		});

		const incrementField = InputDropdown.new({  inputSelector: incrementCol, dataSource: this.selectedTableList.value, boundComponent: this });

		const tableField = InputDropdown.new({
			inputSelector: tableSelecter, 
			dataSource: this.selectedSecretTableList.value,
			boundComponent: this,
			componentFieldName: tableFieldName,
			onSelect: async (table, self) => {
				const data = this.tablesFieldsMap[table], pkRelatedField = self.relatedFields[0], 
					  incrRelatedField = (self.relatedFields.length > 1 ? self.relatedFields[1] : null);
				this.selectedTablesName[self.componentFieldName] = table;
				pkRelatedField.setDataSource(data.map(col => col.column));

				if(incrRelatedField) incrRelatedField.setDataSource(data.map(col => col.column));

				self.relatedFields[0].filterInput.value = '';
			}
		});

		tableField.relatedFields.push(pkField);
		tableField.relatedFields.push(incrementField);
		this.dynamicFields.tables.push(tableField);
		this.dynamicFields.fields.push(pkField);
	}

	setupOnChangeListen(){
		this.database.onChange(newValue => {
			const data = WorkSpaceController.getNode(this.nodeId).data;
			data['database'] = newValue;
		});

		this.selectedDbEngine.onChange(value => {
			const data = WorkSpaceController.getNode(this.nodeId).data;			
			this.setDBIcon(value);
			this.selectedDbEngineDescription = this.databaseEngines.value.find(obj => obj.dialect === value)?.name;
			data['dbengine'] = value;
		});

		this.selectedSecret.onChange(async secretName => {
			// To prevent running through the bellow steps in case the secret is the same
			let database = '', dbengine = '', host = '';
			if(this.importFields.asTemplate) await loadTableList(secretName, database, dbengine, host);

			if(this.wSpaceController.isSubmittingPipeline) return;
			if(this.isImport && this.importFields.changeCount == 0) return this.importFields.changeCount++;
			if(this.secretedSecretTrace == secretName || (this.isImport && !this.importFields.asTemplate)) return;

			this.secretedSecretTrace = secretName;
			this.clearSelectedTablesAndPk();
			this.showLoading = true;
			if(secretName != '') await loadTableList(secretName, database, dbengine, host);
			this.showLoading = false;
		});

		const self  = this;
		async function loadTableList(secretName, database, dbengine, host){
			const data = await WorkspaceService.getConnectionDetails(secretName);
			if('secret_details' in (data || {})){
				const detail = data['secret_details'];
				database = detail?.database, dbengine = detail?.dbengine, host = detail?.host;
				self.tablesFieldsMap = data.tables, self.selectedSecretTableList = Object.keys(data.tables);
				self.database = database, self.selectedDbEngine = dbengine, self.hostName = host;
			}
			WorkSpaceController.getNode(self.nodeId).data['host'] = host;
			self.dynamicFields.tables.forEach(tbl => tbl.setDataSource(self.selectedSecretTableList.value));
		}
	}

	setDBIcon = (db) => {
		if(this.container)
			this.container.querySelector('.database-icon').src = databaseIcons[db == '' ? 'generic' : db];
	}
	
	clearSelectedTablesAndPk(){
		this.getDynamicFieldNames().forEach(field => this[field] = '');
		this.tableName = '', this.primaryKey = '', this.incrementCol = '';
	}

	/** Brings the existing Databases secret */
	async getDBSecrets(){
		const dbSecretType = 1;	
		this.secretList = (await WorkspaceService.listSecrets(dbSecretType)).filter(itm => itm.host != 'None' && itm.bucket == 'no');
	}

	async reloadMe(){
		this.showLoading = true;
		await this.getDBSecrets();
		this.selectedSecret = '';
		this.showLoading = false;
		this.removeAddedTableCallbacks.forEach((func) => func());
	}

	addField(){
		this.dbInputCounter = this.dbInputCounter + 1;
		const tableId = this.dbInputCounter;
		this.newTableField(tableId);
	}

	newTableField = (tableId, value = '', disabled = false, pkValue = '', incrementByVal = '') => 
		addSQLComponentTableField(this, tableId, value, pkValue, disabled, this.isOldUI, incrementByVal);

	async getTables(){
		//getDynamicFields is a map of all fields (with respective values) created through FormHelper.newField 
		const data = WorkSpaceController.getNode(this.nodeId).data;
		const /** @type Array<String> */ dynFields = this.getDynamicFields();

		const tables = { tableName: this.tableName.value };
		const pkFields = { pkName: this.primaryKey.value };
		const incrementCol = { incrementCol: this.incrementCol.value };
		
		for(const [field, val] of Object.entries(dynFields)){
			if(field.trim().startsWith('tableName'))
				tables[field.trim()] = val;
			else if(field.trim().startsWith('incrementCol'))
				incrementCol[field.trim()] = val;
			else
				pkFields[field.trim()] = val;
		}

		data['tables'] = tables;
		data['primaryKeys'] = pkFields;
		data['incrementCols'] = incrementCol;
		data['namespace'] = await UserService.getNamespace();
		data['connectionName'] = this.selectedSecret.value;
		data['isIncremental'] = this.showIncrementalByFields;
	}

	onOutputConnection(){
		SqlDBComponent.handleOutputConnection(this);
		return {
			tables: this.selectedSecretTableList?.value?.map(table => ({ name: table, file: table })),
			sourceNode: this, nodeCount: this.nodeCount.value
		};
	}

	/** @param { InputConnectionType<{}> } param0 */
	onInputConnection({ type, data }){
		SqlDBComponent.handleInputConnection(this, data, type);
	}

	incrementallyUpdate(state){
		const nodeContainer = document.querySelector(`.${this.cmpInternalId}`).parentElement.parentElement;
		
		if(state === true) nodeContainer.style.width = '408px';
		else nodeContainer.style.width = '275px';

		this.showHideIncrementByField(state);
	}

	showHideIncrementByField(state, dynamic){
		const inputs = this.container.querySelectorAll('input[data-id="firstIncrementCol"], .increment-by-field');		
		inputs.forEach(elm => elm.style.display = state ? 'flex' : 'none');
		this.showIncrementalByFields = state;
		if(dynamic && state) this.container.querySelector('.incrementalUpdateToggle').checked = true;
	}

}


class TableAndPKType {

	/** @type { Array<InputDropdown> } */ tables = [];
	/** @type { Array<InputDropdown> } */ fields = [];

}