import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { State, STForm } from "../../../@still/component/type/ComponentType.js";
import { FormHelper } from "../../../@still/helper/form.js";
import { AppTemplate } from "../../../config/app-template.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";

export class CatalogForm extends ViewComponent {

	isPublic = true;

	/** @Prop */ modal;
	/** @Prop */ openModal;
	/** @Prop */ closeModal;
	/** @Prop */ showAddSecrete = false;

	/** @Prop */ dataBaseSettingType = null;
	/** @Prop @type { STForm } */ formRef = null;
	/** @Prop */ dynamicFieldCount = 0;
	/** @Prop */ isDbFirstCall = false;

	/** @type { Workspace } */ $parent;

	connectionName;
	dbEngine;
	dbHost;
	dbPort;
	dbName;
	dbUser;
	firstKey;
	firstValue;

	stAfterInit(){

		this.modal = document.getElementById('modal');
		//this.openModal = document.getElementById('openModal');
		this.closeModal = document.getElementById('closeModal');
		this.handleModalCall();

	}

	changeType(value){
		this.showAddSecrete = true, this.dataBaseSettingType = value;
		if(value == 1){
			if(!this.isDbFirstCall) {
				this.addSecreteGroup(true);
				this.isDbFirstCall = true;
			}
			document.querySelectorAll('.catalog-form-db-fields input:not(.no-required), .catalog-form-db-fields select').forEach(inpt => inpt.setAttribute('required', true));
			document.querySelectorAll('.catalog-form-secret-group input').forEach(inpt => {
				inpt.removeAttribute('required');
				inpt.removeAttribute('(required)');
			});
		}else{
			document.querySelectorAll('.catalog-form-db-fields input, .catalog-form-db-fields select').forEach(inpt => {
				inpt.removeAttribute('required');
				inpt.removeAttribute('(required)');
			});
			document.querySelectorAll('.catalog-form-secret-group input').forEach(inpt => inpt.setAttribute('required', true));
		}

	}

	showDialog(){		
		if(this.modal.style.display !== 'flex')
			this.modal.style.display = 'flex';
		else
			this.modal.style.display = 'none';
	}

	handleModalCall(){
		const self = this;
		//this.openModal.addEventListener('click', () => self.modal.style.display = 'flex');
		this.closeModal.addEventListener('click', () => self.modal.style.display = 'none');
		window.addEventListener('click', (e) => e.target === modal ? self.modal.style.display = 'none' : '');
	}

	addSecreteGroup(initial = false){
		const type = this.dataBaseSettingType == 1 ? 'db' : 'secret';
		const targetForm = type == 'db' ? 'catalog-form-db-fields' : 'catalog-form-secret-group';
		this.dynamicFieldCount++;
		const value = initial ? 'DB_PASSWORD' : '';
		const disabled = initial ? true : false;
		
		let fieldName = `key${this.dynamicFieldCount}-${type}`;
		const secretKeyField = FormHelper.newField(this, this.formRef, fieldName)
			.input({ required: true, placeholder: 'e.g. DB_PASSWORD', className: 'secret-field', disabled, value })
			.element;

		fieldName = `val${this.dynamicFieldCount}-${type}`;
		const secretValField = FormHelper.newField(this, this.formRef, fieldName)
			.input({ required: true, placeholder: 'Enter the secret value', type: 'password' })
			.element;

		this.addSecreteField(secretKeyField, targetForm, null, fieldName);
		this.addSecreteField(secretValField, targetForm, 'val', fieldName, initial);

	}

	addSecreteField = (field, targetForm, type, fieldName, initial) => {
		const div = document.createElement('div');
		div.className = `form-group remove-dyn-field-${fieldName}`;

		if(type == 'val')
			field = this.parseEvents(`
				<span class="${fieldName} hidden-secret-value">
					${field} 
					${!initial ? `<span onclick="inner.removeField('${fieldName}')">x</span>` : ''} 
					<img src="app/assets/imgs/view-svgrepo-com.svg" onclick="inner.viewSecretValue('${fieldName}')">
				<span>`);

		div.innerHTML = field;
		document.querySelector(`.${targetForm}`).appendChild(div);
	}

	viewSecretValue(fieldContainer, secretType){
		const type = this.dataBaseSettingType == 1 ? 'catalog-form-db-fields' : 'catalog-form-secret-group';
		const clsName = fieldContainer == 'initial' ? `.initial-secret-field${secretType == 'db' ? '-db' : ''}` : `.${type} .${fieldContainer}`;
		const currentVal = document.querySelector(`${clsName} input`).type;
		if(currentVal == 'text') {
			document.querySelector(`${clsName} input`).type = 'password';
			document.querySelector(`${clsName} img`).src = 'app/assets/imgs/view-svgrepo-com.svg';
		}else{
			document.querySelector(`${clsName} input`).type = 'text';
			document.querySelector(`${clsName} img`).src = 'app/assets/imgs/view-hide-svgrepo-com.svg';
		}
	}

	removeField = (fieldName) => {
		FormHelper.delField(this,this.formRef,fieldName);
		FormHelper.delField(this,this.formRef,fieldName.replace('val','key'));
		document.querySelectorAll(`.remove-dyn-field-${fieldName}`).forEach(itm => itm.remove());
	};
	

	async createSecret(){
		const validate = await this.formRef.validate();
		console.log(`ERROR COUNT: `, this.formRef.errorCount);

		if(validate){

			dbConfig = {
				'plugin_name': this.dbEngine.value,
				'connection_url': 'postgresql://{{username}}:{{password}}@{{host}}:{{port}}/{{dbname}}',
				'verify_connection': false,
				'username': this.dbUser.value,
				'password': this.firstValue.value,
				'dbname': this.dbName.value,
				'host': this.dbHost.value,
				'port': this.dbPort.value,
				'connectionName': this.connectionName.value
			}
			WorkspaceService.createSecret({ env: this.getDynamicFields(), dbConfig });
			
		}else{
			AppTemplate.toast.error('Please fill all required field');
		}
		
	}

}