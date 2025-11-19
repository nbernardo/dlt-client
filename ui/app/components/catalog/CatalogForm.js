import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { State, STForm } from "../../../@still/component/type/ComponentType.js";
import { FormHelper } from "../../../@still/helper/form.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";
import { AppTemplate } from "../../../config/app-template.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";
import { Workspace } from "../workspace/Workspace.js";
import { handleAddEndpointField, markOrUnmarkAPICatalogRequired, onAPIAuthChange, showHidePaginateEndpoint, viewSecretValue } from "./util/CatalogUtil.js";

export class CatalogForm extends ViewComponent {

	isPublic = true;

	/** @Prop */ modal;
	/** @Prop */ openModal;
	/** @Prop */ closeModal;
	/** @Prop */ showAddSecrete = false;
	/** @Prop */ showServiceNameLbl = false;

	/** @Prop */ dataBaseSettingType = null;
	/** @Prop @type { STForm } */ formRef = null;
	/** @Prop */ dynamicFieldCount = 0;
	/** @Prop */ isDbFirstCall = false;
	/** @Prop */ secretType = 1;
	/** @Prop */ editorId = '_'+UUIDUtil.newId();
	/** @Prop */ isNewSecret = false;
	/** @Prop */ hideCodeEditor = false;
	/** @Prop */ apiAuthType = false;

	/** @type { Workspace } */ $parent;

	// DB catalog/secrets fields
	dbEngine;
	dbHost;
	dbPort;
	dbName;
	dbUser;
	connectionName;
	
	// Bellow State variables are shared between API and DB secret creation
	firstKey;
	firstValue;
	
	// API catalog/secrets variables
	apiConnName;
	apiKeyName;
	apiKeyValue;
	apiTknValue;
	paginationStartField1;
	paginationLimitField1;
	paginationRecPerPage1; //Record per pages
	apiBaseUrl;
	apiEndpointPath1;
	apiEndpointPathPK1;

	endpointCounter = 1;

	stOnRender = ({ type }) => {
		type && (this.secretType = type);
		if(type == 2) this.$parent.controller.loadMonacoEditorDependencies();
	}
	
	async stAfterInit(){
		this.showServiceNameLbl = false;
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
			this.startCodeEditor();
			//this.addSecreteGroup(true, false);
			this.onAPIAuthChange();
		}

		this.dbEngine.onChange(dbEngine => {
			if(dbEngine == 'oracle-database-plugin')
				this.showServiceNameLbl = true;
			else
				this.showServiceNameLbl = false;
		});
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

	resetForm(){
		this.connectionName = '';
		this.dbHost = '';
		this.dbPort = '';
		this.dbName = '';
		this.dbUser = '';
		this.dbEngine = '';
		if(document.querySelector('.first-secret-field')) document.querySelector('.first-secret-field').value = '';
	}

	showDialog(reset = false){

		if(reset){
			this.isNewSecret = true;
			this.resetForm();
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
			document.querySelector('input[data="unique-api-name"]').disabled = false;
			self.dataBaseSettingType = 0;
			self.modal.style.display = 'none';
			self.showAddSecrete = false;
			self.firstKey = '';
			self.firstValue = '';
			document.querySelectorAll('input[name="dbSettingType"]').forEach(opt => opt.checked = false);
			self.isNewSecret = false;
		}
	}

	addSecreteGroup(initial = false, valueRequired = true){
		
		let type = 'secret';
		let targetForm = 'catalog-form-secret-api';

		if(this.dataBaseSettingType !== null){
			type = this.dataBaseSettingType == 1 ? 'db' : 'secret';
			targetForm = type == 'db' ? 'catalog-form-db-fields' : 'catalog-form-secret-group';
		}
		
		this.dynamicFieldCount++;
		const value = targetForm.endsWith('api') ? 'API_TOKEN' : initial ? 'DB_PASSWORD' : '';
		const disabled = initial ? true : false;
		
		let fieldName = `key${this.dynamicFieldCount}-${type}`;
		const secretKeyField = FormHelper.newField(this, this.formRef, fieldName)
			.input({ required: true, placeholder: 'e.g. DB_PASSWORD', className: 'secret-field', disabled, value })
			.element;

		fieldName = `val${this.dynamicFieldCount}-${type}`;
		const secretValField = FormHelper.newField(this, this.formRef, fieldName)
			.input({ required: valueRequired, placeholder: 'Enter the secret value', type: 'password', className: initial ? 'first-secret-field' : '' })
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

	viewSecretValue = (fieldContainer) => viewSecretValue(fieldContainer, this.dataBaseSettingType, this.secretType);

	removeField = (fieldName) => {
		FormHelper.delField(this,this.formRef,fieldName);
		FormHelper.delField(this,this.formRef,fieldName.replace('val','key'));
		document.querySelectorAll(`.remove-dyn-field-${fieldName}`).forEach(itm => itm.remove());
	};
	

	async createSecret(){
		const validate = await this.formRef.validate(); 
		let dbConfig = null, apiSettings = null, updatingSecret;
		//console.log(`API SETTINGS IS: `, this.parseAPICatalogFields());
		//return console.log(`TOTAL ERRORS: `, this.formRef.errorCount);
		
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
				updatingSecret = this.getUpdatingSecret();

				if(updatingSecret.updatingId && this.isNewSecret){
					return AppTemplate.toast.error(`Secret with name ${this.connectionName.value} already exists`);
				}
			}else{
				apiSettings = {
					...this.parseAPICatalogFields(), keyName: this.apiKeyName.value, keyValue: this.apiKeyValue.value, 
					token: this.apiTknValue.value, apiBaseUrl: this.apiBaseUrl.value,
				};
			}
			const connectionName = this.dataBaseSettingType != null ? this.connectionName.value : this.apiConnName.value;
			const result = await WorkspaceService.createSecret({ 
				env: this.getDynamicFields(), dbConfig, apiSettings, connectionName
			});

			if(result === true && this.dataBaseSettingType != null)
				this.updateLeftMenuSecretList(updatingSecret);
		}else{
			AppTemplate.toast.error('Please fill all required field');
		}
		
	}

	getUpdatingSecret(){
		const updatedSecrets = [...this.$parent.controller.leftTab.dbSecretsList.value];
		let updatingId = null;
		for(const id in updatedSecrets){
			if(updatedSecrets[id].name === this.connectionName.value) 
				updatingId = id;
		}
		return { updatingId, updatedSecrets };
	}

	updateLeftMenuSecretList({ updatingId, updatedSecrets }){
		const host = this.dataBaseSettingType == 2 ? 'None' : this.dbHost.value;
		if(updatingId !== null) updatedSecrets[updatingId].host = host;
		if(updatingId === null) updatedSecrets.push({ name: this.connectionName.value, host });
		this.$parent.controller.leftTab.dbSecretsList = updatedSecrets;
		this.resetForm();
	}

	onAPIAuthChange = (type = null) => this.apiAuthType = onAPIAuthChange(type);

	/** @Prop */ useAuth = false;
	setUseAuth = (value) => {
		if(!value) {
			this.apiKeyName = null;
			this.apiKeyValue = null;
			this.apiTknValue = null;
			this.onAPIAuthChange(null);
			document.querySelector('.use-auth-secret-input').value = '';
		}
		document.querySelector('.catalog-form-secret-api .use-auth-secret-input').style.display = value ? '' : 'none';
	}

	/** @Prop */ usePagination = false;
	setUsePagination = (value) => {
		if(!value){
			this.paginationStartField = null;
			this.paginationEndField = null;
			this.paginationBatch = null;
		}
		markOrUnmarkAPICatalogRequired();
	}

	addEndpointFields = () => {
		this.endpointCounter = this.endpointCounter.value + 1;
		handleAddEndpointField(this.endpointCounter.value, this);
	}

	parseAPICatalogFields(){

		const endPointsGroup = {
			paginationStartField: [this.paginationStartField1.value],
			paginationLimitField: [this.paginationLimitField1.value],
			paginationRecPerPage: [this.paginationRecPerPage1.value],
			apiEndpointPath: [this.apiEndpointPath1.value],
			apiEndpointPathPK : [this.apiEndpointPathPK1.value],
		}

		const dynamicFields = this.getDynamicFields();
		const validFieldNames = [
			'apiEndpointPath','apiEndpointPathPK',
			'paginationStartField','paginationLimitField','paginationRecPerPage'
		]

		console.log(`THIS IS RHE VAKUES: `, dynamicFields);
		

		for(let x = 2; x <= this.endpointCounter; x++){
			for(const field of validFieldNames){
				const fieldValue = dynamicFields[`${field}${x}`] || '';
				endPointsGroup[field].push(fieldValue);
			}
		}

		return { 
			apiBaseUrl: this.apiBaseUrl.value, apiKeyName: this.apiKeyName.value,
			apiKeyValue: this.apiKeyValue.value, apiTknValue: this.apiTknValue.value,
			endPointsGroup
		}
	}

	showPaginateEndpoint = () => showHidePaginateEndpoint(1, true);
	hidePaginateEndpoint = () => {
		this.paginationStartField1 = '';
		this.paginationLimitField1 = '';
		this.paginationRecPerPage1 = '';
		showHidePaginateEndpoint(1, false);
	}

}