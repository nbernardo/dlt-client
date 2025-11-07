import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { State, STForm } from "../../../@still/component/type/ComponentType.js";
import { FormHelper } from "../../../@still/helper/form.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";
import { AppTemplate } from "../../../config/app-template.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";
import { Workspace } from "../workspace/Workspace.js";

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
	/** @Prop */ secretType = 1;
	/** @Prop */ editorId = '_'+UUIDUtil.newId();

	/** @type { Workspace } */ $parent;

	dbEngine;
	dbHost;
	dbPort;
	dbName;
	dbUser;
	
	// Bellow State variables are shared between API and DB secret creation
	firstKey;
	firstValue;
	connectionName;

	stOnRender = ({ type }) => {
		type && (this.secretType = type);
		if(type == 2) this.$parent.controller.loadMonacoEditorDependencies();
	}
	
	async stAfterInit(){

		this.modal = document.getElementById('modal');
		//this.openModal = document.getElementById('openModal');
		this.closeModal = document.getElementById('closeModal');
		this.handleModalCall();
		const secretList = await WorkspaceService.listSecrets(this.secretType);
		
		if(this.secretType == 2)
			this.$parent.controller.leftTab.apiSecretsList = secretList;
		else
			this.$parent.controller.leftTab.dbSecretsList = secretList;
		
		this.$parent.controller.leftTab.showLoading = false;

		if(this.secretType == 2) {
			this.markAPIFieldsAsRequired();
			this.startCodeEditor();
			this.addSecreteGroup(true);
		}
	}

	startCodeEditor(){
		this.editor = this.$parent.controller.loadMonadoEditor(
			document.getElementById(this.editorId), { lang: 'json' }
		);
		this.editor.setValue(`{ \n\t"apiName": "TO BE DEFINED" \n}`);
	}

	editSecret(type, secretData){
		this.connectionName = secretData.secretName;
		
		if(this.secretType == 1){
			let selectedOption = 0;
			this.showDialog();
			if(!('connection_url' in secretData)){
				selectedOption = 1;
				this.firstKey = secretData.secretName;
				this.firstValue = secretData[secretData.secretName];
				document.querySelector('.save-secret-btn').style.display = 'none';
				document.querySelector('.btn-add-secret').disabled = true;
			}
			
			document.querySelectorAll('.database-settings-type input')[selectedOption].click();
			if('connection_url' in secretData){
				this.dataBaseSettingType = 1;
				this.dbHost = secretData.host;
				this.dbPort = secretData.port;
				this.dbName = secretData.database;
				this.dbUser = secretData.username;
				this.dbEngine = secretData?.dbengine+'-database-plugin';
				
				document.querySelector('.first-secret-field').value = secretData.password;
				document.querySelector('.db-connection-name').disabled = true;
				document.querySelectorAll('.database-settings-type input')[selectedOption == 1 ? 0 : 1].disabled = true;
			}
		}

		if(this.secretType == 2){
			this.showDialog();
			document.querySelector('.unique-api-name').disabled = true;
			document.querySelector('.catalog-form-secret-api .first-secret-field').value = secretData.env['val1-secret'];
			this.editor.setValue(secretData.apiSettings);
		}
		document.querySelectorAll('input[name="dbSettingType"]').forEach(opt => opt.disabled = true);
	}

	markAPIFieldsAsRequired(){
		document.querySelectorAll('.catalog-form-secret-api input').forEach(inpt => inpt.setAttribute('required', true));
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

	showDialog(reset = false){

		if(reset){
			this.connectionName = '';
			this.dbHost = '';
			this.dbPort = '';
			this.dbName = '';
			this.dbUser = '';
			this.dbEngine = '';
			if(document.querySelector('.first-secret-field')) document.querySelector('.first-secret-field').value = '';
		}

		document.querySelector('.db-connection-name').disabled = false;
		document.querySelectorAll('.database-settings-type input').forEach(opt => opt.disabled = false);
		if(this.modal.style.display !== 'flex')
			this.modal.style.display = 'flex';
		else
			this.modal.style.display = 'none';
	}

	handleModalCall(){
		const self = this;
		//this.openModal.addEventListener('click', () => self.modal.style.display = 'flex');
		this.closeModal.addEventListener('click', () => resetForm());
		window.addEventListener('click', (e) => e.target === modal ? resetForm() : '');

		function resetForm(){
			document.querySelector('.save-secret-btn').style.display = '';
			document.querySelector('.btn-add-secret').disabled = false;
			self.dataBaseSettingType = 0;
			self.modal.style.display = 'none';
			self.showAddSecrete = false;
			self.firstKey = '';
			self.firstValue = '';
			document.querySelectorAll('input[name="dbSettingType"]').forEach(opt => opt.checked = false);
		}
	}

	addSecreteGroup(initial = false){
		
		let type = 'secret';
		let targetForm = 'catalog-form-secret-api';

		if(this.dataBaseSettingType !== null){
			type = this.dataBaseSettingType == 1 ? 'db' : 'secret';
			targetForm = type == 'db' ? 'catalog-form-db-fields' : 'catalog-form-secret-group';
		}
		
		this.dynamicFieldCount++;
		const value = targetForm.endsWith('api') ? 'API_KEY' : initial ? 'DB_PASSWORD' : '';
		const disabled = initial ? true : false;
		
		let fieldName = `key${this.dynamicFieldCount}-${type}`;
		const secretKeyField = FormHelper.newField(this, this.formRef, fieldName)
			.input({ required: true, placeholder: 'e.g. DB_PASSWORD', className: 'secret-field', disabled, value })
			.element;

		fieldName = `val${this.dynamicFieldCount}-${type}`;
		const secretValField = FormHelper.newField(this, this.formRef, fieldName)
			.input({ required: true, placeholder: 'Enter the secret value', type: 'password', className: initial ? 'first-secret-field' : '' })
			.element;

		this.addSecreteField(secretKeyField, targetForm, null, fieldName);
		this.addSecreteField(secretValField, targetForm, 'val', fieldName, initial);

	}

	addSecreteField = (field, targetForm, type, fieldName, initial) => {

		const div = document.createElement('div');
		div.className = `form-group remove-dyn-field-${fieldName}`;
		const isAPISecret = this.dataBaseSettingType == null;
		const subContainer = isAPISecret ? '.modal-body' : '';

		if(type == 'val')
			field = this.parseEvents(`
				<span class="${fieldName} hidden-secret-value">
					${field} 
					${!initial ? `<span onclick="inner.removeField('${fieldName}')">x</span>` : ''} 
					<img src="app/assets/imgs/view-svgrepo-com.svg" onclick="inner.viewSecretValue('${fieldName}')">
				<span>`);

		div.innerHTML = field;
		document.querySelector(`.${targetForm} ${subContainer}`).appendChild(div);
	}

	viewSecretValue(fieldContainer){

		let type = 'catalog-form-secret-api', secretType = '-api';
		if(this.dataBaseSettingType !== null && this.secretType != 2){
			type = this.dataBaseSettingType == 1 ? 'catalog-form-db-fields' : 'catalog-form-secret-group';
			secretType = this.dataBaseSettingType == 1 ? '-db' : '';
		}

		const clsName = fieldContainer == 'initial' ? `.initial-secret-field${secretType}` : `.${type} .${fieldContainer}`;
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
		let dbConfig = null, apiSettings = null;
		
		if(this.secretType != 2 && this.dataBaseSettingType == null) 
			return AppTemplate.toast.error('Please select the secret type');

		if(validate){

			if(this.dataBaseSettingType != null){
				if(this.dataBaseSettingType == 2){
					const allKeys = Object.keys(this.getDynamicFields()).filter(itm => itm.startsWith('key'));
					dbConfig = {
						secretsOnly: true,
						connectionName: this.connectionName.value,
						secrets: [ { 
							[this.firstKey.value]: this.firstValue.value }, 
							...allKeys.map(k => ({ [this.getDynamicFields()[k]]: this.getDynamicFields()[k.replace('key','val')] }))
						]
					};
				}else{
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
				}
			}else
				apiSettings = this.editor.getValue();
			
			WorkspaceService.createSecret({ 
				env: this.getDynamicFields(), dbConfig, apiSettings, 'connectionName': this.connectionName.value 
			});
			
		}else{
			AppTemplate.toast.error('Please fill all required field');
		}
		
	}

}