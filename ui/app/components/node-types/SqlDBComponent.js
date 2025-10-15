import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { STForm } from "../../../@still/component/type/ComponentType.js";
import { FormHelper } from "../../../@still/helper/form.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";
import { WorkSpaceController } from "../../controller/WorkSpaceController.js";

export class SqlDBComponent extends ViewComponent {

	isPublic = true;

	label = 'SQL DB Source';
	databaseEngines = [
		{ name: 'MySQL', dialect: 'mysql' },
		{ name: 'Postgress', dialect: 'postgres' },
		{ name: 'Oracle', dialect: 'oracle' },
		{ name: 'SQL Server', dialect: 'mssql' }
	]

	/** @Prop */
	inConnectors = 1;
	/** @Prop */
	outConnectors = 1;
	/** @Prop */
	nodeId;
	/** @Prop */
	dbInputCounter = 1;
	/** @Prop @type { STForm } */	
	formRef;
	database;
	tableName;
	selectedDbEngine;
	primaryKey;

	/** @Prop */ isImport = false;
	/** @Prop */ formWrapClass = '_'+UUIDUtil.newId();

	/** @Prop @type { STForm } */
	anotherForm;
	databaseC;
	tableNameC;

	// tables hold all tables name when importing/reading
	// An existing pipeline by calling the API
	/** @Prop @type { Map } */ tables;

	/**
	 * @Inject @Path services/
	 * @type { WorkSpaceController } */
	wSpaceController;

	/**
	 * The id will be passed when instantiating SqlDBComponent dinamically
	 * through the Component.new(type, param) where for para nodeId 
	 * will be passed
	 * */
	stOnRender({ nodeId, isImport, tables }){
		this.nodeId = nodeId;
		this.isImport = isImport;
		this.tables = tables;		
	}

	stAfterInit(){

		// When importing, it might take some time for things to be ready, the the subcrib to on change
		// won't be automatically, setupOnChangeListen() will be called explicitly in the WorkSpaceController
		//if(this.isImport !== false){
		//	this.setupOnChangeListen();
		//}

		if(this.isImport === true){	
			// At this point the WorkSpaceController was loaded by WorkSpace component
			// hance no this.wSpaceController.on('load') subscrtiption is needed
			this.wSpaceController.disableNodeFormInputs(this.formWrapClass);

			const disable = true;
			const allTables = Object.values(this.tables);
			// Assign the first table
			this.tableName = this.tables['tableName'];
			// Assign remaining tables if more than one in the pipeline
			allTables.slice(1).forEach((tblName, idx) => this.newTableField(idx + 2, tblName, disable));
			this.dbInputCounter = allTables.length;
		}

		this.setupOnChangeListen();

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

	}

	addField(){
		this.dbInputCounter = this.dbInputCounter + 1;
		const tableId = this.dbInputCounter;
		this.newTableField(tableId);
	}

	newTableField(tableId, value = '', disabled = false){
		let fieldName = 'tableName' + tableId, placeholder = 'Enter table '+tableId+' name';
		const table = FormHelper
			.newField(this, this.formRef, fieldName, value)
			.input({ required: true, placeholder, validator: 'alphanumeric', value, disabled })
			//Add will add in the form which reference was specified (2nd param of newField)
			//.add((inpt) => `<div style="padding-top:5px;">${inpt}</div>`);
			.element;
		
		fieldName = 'primaryKey' + tableId, placeholder = 'PK Field';
		const pkField = FormHelper
			.newField(this, this.formRef, fieldName, value)
			.input({ required: true, placeholder, validator: 'alphanumeric', value, disabled })
			.element;

		const div = document.createElement('div');
		div.style.marginTop = '3px';
		div.className = 'table-detailes';
		div.innerHTML = `${table}${pkField}`;		
		document.querySelector(`.${this.formWrapClass} form`).appendChild(div);

	}

	getTables(){
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
	}

	showTable(){
		console.log(this.getDynamicFields());
	}
}