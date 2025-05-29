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

	/** @Prop @type { STForm } */
	anotherForm;
	databaseC;
	tableNameC;

	stOnRender(nodeId){
		this.nodeId = nodeId;
	}

	stAfterInit(){
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
			.getInput({ required: true, placeholder, validator: 'alhpanumeric' })
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