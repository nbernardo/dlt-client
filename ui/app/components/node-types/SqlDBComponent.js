import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { STForm } from "../../../@still/component/type/ComponentType.js";
import { FormHelper } from "../../../@still/helper/form.js";
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

	/** @Prop */ isImport = false;

	/** @Prop @type { STForm } */
	anotherForm;
	databaseC;
	tableNameC;

	/**
	 * The id will be passed when instantiating SqlDBComponent dinamically
	 * through the Component.new(type, param) where for para nodeId 
	 * will be passed
	 * */
	stOnRender({ nodeId, isImport }){
		this.nodeId = nodeId;
		this.isImport = isImport;
	}

	stAfterInit(){

		// When importing, it might take some time for things to be ready, the the subcrib to on change
		// won't be automatically, setupOnChangeListen() will be called explicitly in the WorkSpaceController
		if(this.isImport !== false){
			this.setupOnChangeListen();
		}

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
		const fieldName = 'tableName' + tableId;
		const placeholder = 'Enter table '+tableId+' name';

		FormHelper
			.newField(this, this.formRef, fieldName)
			.getInput({ required: true, placeholder, validator: 'alphanumeric' })
			//Add will add in the form which reference was specified (2nd param of newField)
			.add((inpt) => `<div style="padding-top:5px;">${inpt}</div>`);	
	}

	getTables(){
		//getDynamicFields is a map of all fields (with respective values) created through FormHelper.newField 
		const data = WorkSpaceController.getNode(this.nodeId).data;
		const dynFields = this.getDynamicFields();
		const tables = { tableName: this.tableName.value, ...dynFields };
		data['tables'] = tables;
	}

	showTable(){
		console.log(this.getTables());
	}
}