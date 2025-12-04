import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { STForm } from "../../../@still/component/type/ComponentType.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";
import { WorkSpaceController } from "../../controller/WorkSpaceController.js";
import { UserService } from "../../services/UserService.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";
import { InputDropdown } from "../../util/InputDropdownUtil.js";
import { NodeTypeInterface } from "./mixin/NodeTypeInterface.js";
import { addSQLComponentTableField } from "./util/formUtil.js";

/** @implements { NodeTypeInterface } */
export class SqlDBComponent extends ViewComponent {

	isPublic = true;

	label = 'SQL DB Source';
	databaseEngines = [
		{ name: 'MySQL', dialect: 'mysql' },
		{ name: 'Postgress', dialect: 'postgresql' },
		{ name: 'Oracle', dialect: 'oracle' },
		{ name: 'SQL Server', dialect: 'mssql' }
	]

	/** @Prop */ inConnectors = 1;
	/** @Prop */ outConnectors = 1;
	/** @Prop */ nodeId;
	/** @Prop */ dbInputCounter = 1;
	/** @Prop @type { STForm } */ formRef;
	/** @Prop */ isOldUI;
	/** @Prop @type { TableAndPKType } */ dynamicFields;

	selectedSecretTableList = [];
	selectedTableList = [];
	database;
	tableName;
	selectedDbEngine;
	selectedSecret;
	primaryKey;
	secretList = [];
	hostName = 'None';

	/** @Prop */ isImport = false;
	/** @Prop */ formWrapClass = '_'+UUIDUtil.newId();

	/** @Prop @type { STForm } */ anotherForm;
	/** @Prop */ showLoading = false;
	


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
		const { nodeId, isImport, tables, primaryKeys, database, dbengine, connectionName } = data;
		
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
		// When importing, it might take some time for things to be ready, the the subcrib to on change
		// won't be automatically, setupOnChangeListen() will be called explicitly in the WorkSpaceController
		//if(this.isImport !== false){
		//	this.setupOnChangeListen();
		//}
		this.dynamicFields = new TableAndPKType();
		if(this.isImport === true){	
			// At this point the WorkSpaceController was loaded by WorkSpace component
			// hance no this.wSpaceController.on('load') subscrtiption is needed
			this.wSpaceController.disableNodeFormInputs(this.formWrapClass);

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
			this.selectedSecret = this.importFields.connectionName;
			this.hostName = this.importFields.host || 'None';
			document.querySelector('.add-table-buttons').disabled = true;
		}

		this.setupOnChangeListen();
		const htmlTableInputSelector = 'input[data-id="firstTable"]', 
			  htmlPkInputSelector = 'input[data-id="firstPK"]';

		if(!this.isOldUI) this.handleTableFieldsDropdown(htmlTableInputSelector, htmlPkInputSelector);

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
				const data = await WorkspaceService.getDBTableDetails(this.selectedDbEngine.value, this.selectedSecret.value ,table);
								
				const pkRelatedField = self.relatedFields[0];
				pkRelatedField.setDataSource(data.fields);
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
			data['dbengine'] = value;
		});

		this.selectedSecret.onChange(async secretName => {
			this.clearSelectedTablesAndPk();
			this.showLoading = true;
			let database = '', dbengine = '', host = '';
			if(secretName != ''){
				const data = await WorkspaceService.getConnectionDetails(secretName);

				const detail = data['secret_details'];
				database = detail?.database, dbengine = detail?.dbengine, host = detail?.host;
				this.selectedSecretTableList = data.tables;
				WorkSpaceController.getNode(this.nodeId).data['host'] = host;
				this.dynamicFields.tables.forEach(tbl => {
					tbl.setDataSource(data.tables);
				});

			}
			this.database = database;
			this.selectedDbEngine = dbengine;
			this.hostName = host;
			this.showLoading = false;
		})
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
		return null;
	}

}


class TableAndPKType {

	/** @type { Array<InputDropdown> } */ tables = [];
	/** @type { Array<InputDropdown> } */ fields = [];

}