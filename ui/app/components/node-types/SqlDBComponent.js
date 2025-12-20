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
import { NodeUtil } from "./util/nodeUtil.js";

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

	selectedSecretTableList = [];
	selectedTableList = [];
	database = 'Not selected';
	tableName;
	selectedDbEngine = 'Not selected';
	selectedDbEngineDescription = 'Not selected';
	selectedSecret;
	primaryKey;
	secretList = [];
	hostName = 'None';
	nodeCount = '';

	/** @Prop */ isImport = false;
	/** @Prop */ formWrapClass = '_'+UUIDUtil.newId();

	/** @Prop @type { STForm } */ anotherForm;
	/** @Prop */ showLoading = false;
	/** @Prop */ secretedSecretTrace = null;
	/** @Prop */ aiGenerated = null;
	
	// tables and primaryKeys hold all tables name when importing/reading
	// An existing pipeline by calling the API
	/** @Prop @type { Map } */ tables;
	/** @Prop @type { Map } */ primaryKeys;
	/** @Prop */ importFields;

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
		const { nodeId, isImport, tables, primaryKeys, database, dbengine, connectionName, aiGenerated } = data;		
		this.aiGenerated = aiGenerated;
		this.nodeId = nodeId;
		this.isImport = isImport;
		this.tables = tables;
		this.primaryKeys = primaryKeys;	
		this.importFields = { database, dbengine, connectionName };
		if(data?.host) this.importFields.host = data.host;
	}

	async stAfterInit(){
		await this.getDBSecrets();
		this.isOldUI = this.templateUrl?.includes('SqlDBComponent_old.html');
		this.selectedSecretTableList = [];

		this.dynamicFields = new TableAndPKType();
		this.setupOnChangeListen();

		if(this.isImport === true) this.handleImportAssignement();
		if(this.aiGenerated === true) this.handleAiGenerated();
		
		const htmlTableInputSelector = 'input[data-id="firstTable"]', 
			  htmlPkInputSelector = 'input[data-id="firstPK"]';

		if(!this.isOldUI) this.handleTableFieldsDropdown(htmlTableInputSelector, htmlPkInputSelector);

	}

	handleImportAssignement(){
		// At this point the WorkSpaceController was loaded by WorkSpace component
		// hance no this.wSpaceController.on('load') subscrtiption is needed
		this.wSpaceController.disableNodeFormInputs(this.formWrapClass);
		this.notifyReadiness();

		const disable = true;
		const allTables = Object.values(this.tables);
		const allKeys = Object.values(this.primaryKeys);

		// Assign the first table
		this.tableName = this.tables['tableName'];
		this.primaryKey = allKeys[0];
		// Assign remaining tables if more than one in the pipeline
		allTables.slice(1).forEach((tblName, idx) => this.newTableField(idx + 2, tblName, disable));
		this.dbInputCounter = allTables.length;
		this.selectedDbEngine = this.importFields.dbengine;
		this.setDBIcon(this.selectedDbEngine);
		this.selectedSecret = this.importFields.connectionName;
		this.hostName = this.importFields.host || 'None';
		document.querySelector('.add-table-buttons').disabled = true;
	}

	handleAiGenerated(){
		this.selectedSecret = this.importFields.connectionName || '';
	}

	handleTableFieldsDropdown(tableSelecter, pkSelecter, tableFieldName, pkFieldName){

		const pkField = InputDropdown.new({ 
			inputSelector: pkSelecter, dataSource: this.selectedTableList.value,
			boundComponent: this, componentFieldName: pkFieldName
		});

		const tableField = InputDropdown.new({
			inputSelector: tableSelecter, 
			dataSource: this.selectedSecretTableList.value,
			boundComponent: this,
			componentFieldName: tableFieldName,
			onSelect: async (table, self) => {
				const data = this.tablesFieldsMap[table];
				
				const pkRelatedField = self.relatedFields[0];
				pkRelatedField.setDataSource(data.map(col => col.column));
			}
		});

		tableField.relatedFields.push(pkField);
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
			this.selectedDbEngineDescription = this.databaseEngines.value.find(obj => obj.dialect === value).name;
			data['dbengine'] = value;
		});

		this.selectedSecret.onChange(async secretName => {
			// To prevent running through the bellow steps in case the secret is the same
			if(this.secretedSecretTrace == secretName || this.isImport) return;

			this.secretedSecretTrace = secretName;
			this.clearSelectedTablesAndPk();
			this.showLoading = true;
			let database = '', dbengine = '', host = '';
			if(secretName != ''){
				const data = await WorkspaceService.getConnectionDetails(secretName);

				const detail = data['secret_details'];
				if('secret_details' in (data || {})){
					database = detail?.database, dbengine = detail?.dbengine, host = detail?.host;
					this.tablesFieldsMap = data.tables;
					this.selectedSecretTableList = Object.keys(data.tables);
				}
				
				WorkSpaceController.getNode(this.nodeId).data['host'] = host;
				this.dynamicFields.tables.forEach(tbl => {
					tbl.setDataSource(this.selectedSecretTableList.value);
				});

			}
			this.database = database;
			this.selectedDbEngine = dbengine;
			this.hostName = host;
			this.showLoading = false;
		})
	}

	setDBIcon = (db) => {
		if(document.querySelector(`.${this.cmpInternalId}`)){
			document.querySelector(`.${this.cmpInternalId}`)
				.querySelector('.database-icon').src = databaseIcons[db == '' ? 'generic' : db];
		}
	}
	
	clearSelectedTablesAndPk(){
		this.getDynamicFieldNames().forEach(field => this[field] = '');
		this.tableName = '', this.primaryKey = '';
	}

	/** Brings the existing Databases secret */
	async getDBSecrets(){
		const dbSecretType = 1;		
		this.secretList = (await WorkspaceService.listSecrets(dbSecretType)).filter(itm => itm.host != 'None');
	}

	addField(){
		this.dbInputCounter = this.dbInputCounter + 1;
		const tableId = this.dbInputCounter;
		this.newTableField(tableId);
	}

	newTableField = (tableId, value = '', disabled = false) => 
		addSQLComponentTableField(this, tableId, value, disabled, this.isOldUI);

	async getTables(){
		//getDynamicFields is a map of all fields (with respective values) created through FormHelper.newField 
		const data = WorkSpaceController.getNode(this.nodeId).data;
		const /** @type Array<String> */ dynFields = this.getDynamicFields();

		const tables = { tableName: this.tableName.value };
		const pkFields = { pkName: this.primaryKey.value };
		
		for(const [field, val] of Object.entries(dynFields)){
			if(field.trim().startsWith('tableName'))
				tables[field.trim()] = val;
			else
				pkFields[field.trim()] = val;
		}

		data['tables'] = tables;
		data['primaryKeys'] = pkFields;
		data['namespace'] = await UserService.getNamespace();
		data['connectionName'] = this.selectedSecret.value;
	}

	async showTable(){
		await this.formRef.validate();
		console.log(this.formRef.errorCount);
	}

	onOutputConnection(){
		NodeUtil.handleOutputConnection(this);
		return {
			tables: this.selectedSecretTableList?.value?.map(table => ({ name: table, file: table })),
			sourceNode: this,
			nodeCount: this.nodeCount.value
		};
	}

	/** @param { InputConnectionType<{}> } param0 */
	onInputConnection({ type, data }){
		NodeUtil.handleInputConnection(this, data, type);
	}

}


class TableAndPKType {

	/** @type { Array<InputDropdown> } */ tables = [];
	/** @type { Array<InputDropdown> } */ fields = [];

}