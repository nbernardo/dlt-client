import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { State, STForm } from "../../../@still/component/type/ComponentType.js";
import { FormHelper } from "../../../@still/helper/form.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";
import { AppTemplate } from "../../../config/app-template.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";
import { Workspace } from "../workspace/Workspace.js";
import { CatalogEndpointType, generateDsnDescriptor, handleAddEndpointField, onAPIAuthChange, parseEndpointPath, showHidePaginateEndpoint, handleShowHideWalletFields, viewSecretValue } from "./util/CatalogUtil.js";

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
	/** @Prop @type { Array<HTMLElement> } */ dynamicEndpointsDelButtons = [];
	/** @type { Workspace } */ $parent;
	/** @Prop */ editorPlaceholder = null;
	/** @Prop */ showKeyFileFields = false;
	/** @Prop */ showTestConnection = false;

	// DB catalog/secrets fields
	dbEngine;
	dbHost;
	dbPort;
	dbName;
	dbUser;
	dbConnectionParams = '';
	checkConnection = '';
	walletPassword;
	walletFile;
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
	paginationRecPerPage1 = 100; //Record per pages
	apiBaseUrl;
	apiEndpointPath1;
	apiEndpointPathPK1;
	apiEndpointDS1; //To specify the field on the API response where data lies
	fullEndpointPath = '';
	/** @Prop */ endPointEditorContent;
	/** @Prop */ isDbConnEditing = false;

	endpointCounter = 1;

	stOnRender = ({ type }) => {
		type && (this.secretType = type);
		if(type == 2) this.$parent.controller.loadMonacoEditorDependencies();
	}
	
	async stAfterInit(){
		this.endPointEditorContent = {};
		this.editorPlaceholder = null;
		this.showServiceNameLbl = false;
		this.showTestConnection = false;
		this.dynamicEndpointsDelButtons = [];
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
			this.onAPIAuthChange();
			this.dataBaseSettingType = null;
		}

		this.dbEngine.onChange(dbEngine => {
			if(dbEngine == 'oracle-database-plugin')
				return this.showServiceNameLbl = true;
			this.showServiceNameLbl = false;
		});
		
		this.onEndpointUpdate();
	}

	showEditor = (placeId) => {
		const placeholderPrefix = 'api-code-editor-placeholder';
		const container = document.querySelector(`.${placeholderPrefix}${placeId}`);
		if(this.editorPlaceholder == placeId){
			this.editor = null;
			this.editorPlaceholder = null;
			container.style="height: 0px;";
			container.innerHTML = '';
			return;
		}else{
			if(this.editorPlaceholder !== undefined && this.editorPlaceholder !== null){
				const prevContainer = document.querySelector(`.${placeholderPrefix}${this.editorPlaceholder}`);
				prevContainer.style="height: 0px;";
				prevContainer.innerHTML = '';
			}
			this.editorPlaceholder = placeId;
			this.editor = this.$parent.controller.loadMonacoEditor(container, { lang: 'json', theme: 'vs-light' });
			container.style="height: 80px; margin-top: 11px;"
			let params = this.endPointEditorContent[placeId] || `{ \t"param1": "param1 value" }`;

			this.editor.setValue(params);

			this.editor.onDidChangeModelContent(() => {
				if(this.editor.getValue() === '')
					this.endPointEditorContent[placeId] = {};
				else
					this.endPointEditorContent[placeId] = this.editor.getValue();
			});
		}
	}

	onEndpointUpdate(){
		const self = this;
		function updateFullPath(){

			const path = self.apiEndpointPath1.value;
			const offset = self.paginationStartField1.value;
			const limit = self.paginationLimitField1.value;
			const batchSize = self.paginationRecPerPage1.value;
			self.fullEndpointPath = parseEndpointPath(path, offset, limit, batchSize)

		}

		this.apiEndpointPath1.onChange(() => updateFullPath());
		this.paginationStartField1.onChange(() => updateFullPath());
		this.paginationLimitField1.onChange(() => updateFullPath());
		this.paginationRecPerPage1.onChange(() => updateFullPath());
	}

	editSecret(type, secretData){
		this.connectionName = secretData.secretName;
		
		if(this.secretType == 1){
			let selectedOption = 0;
			this.showTestConnection = true;
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
				this.firstValue = secretData.password;
				this.dbConnectionParams = secretData.dbConnectionParams;

				document.querySelector('.first-secret-field').value = secretData.password;
				document.querySelector('.db-connection-name').disabled = true;
				document.querySelectorAll('.database-settings-type input')[selectedOption == 1 ? 0 : 1].disabled = true;
			}
			this.isDbConnEditing = true;
		}

		if(this.secretType == 2){
			if(secretData.apiSettings.apiKeyName.trim() !== ''){
				this.apiKeyName = secretData.apiSettings.apiKeyName;
				this.apiKeyValue = secretData.apiSettings.apiKeyValue;
				document.querySelector('.use-auth-secret-input').value = 'api-key';
				this.onAPIAuthChange('api-key');
				document.querySelector('.use-auth-checkbox').click();
			}else if(secretData.apiSettings.apiTknValue.trim() !== ''){
				this.apiTknValue = secretData.apiSettings.apiTknValue;
				document.querySelector('.use-auth-secret-input').value = 'bearer-token';
				this.onAPIAuthChange('bearer-token');
				document.querySelector('.use-auth-checkbox').click();
			}

			this.dataBaseSettingType = null;
			const { 
				apiEndpointPath, apiEndpointPathPK, paginationLimitField, 
				paginationRecPerPage, paginationStartField, apiEndpointDS
			} = secretData.apiSettings.endPointsGroup;

			this.apiEndpointPath1 = apiEndpointPath[0];
			this.apiEndpointPathPK1 = apiEndpointPathPK[0];
			this.apiEndpointDS1 = apiEndpointDS[0];

			if(paginationStartField[0] !== ''){
				document.querySelector('input[name="userPagination1"]').checked = true;
				this.showPaginateEndpoint();
				this.paginationStartField1 = paginationStartField[0];
				this.paginationLimitField1 = paginationLimitField[0];
				this.paginationRecPerPage1 = paginationRecPerPage[0];
			}

			this.endpointCounter = 1;
			for(const idx in paginationStartField.slice(1)){
				const index = Number(idx) + 1;

				if(apiEndpointPath[index]){

					/** @type { CatalogEndpointType } */
					const details = { 
						apiEndpointPath: apiEndpointPath[index],
						apiEndpointPathPK: apiEndpointPathPK[index],
						paginationStartField: paginationStartField[index],
						paginationLimitField: paginationLimitField[index],
						paginationRecPerPage: paginationRecPerPage[index],
						apiEndpointDS: apiEndpointDS[index],
					};				
					this.addEndpointFields(details);

				}
			}

			this.apiBaseUrl = secretData.apiSettings.apiBaseUrl;
			this.apiConnName = secretData.connectionName;
			this.showDialog();
			document.querySelector('.unique-api-name').disabled = true;
			//document.querySelector('.catalog-form-secret-api .first-secret-field').value = secretData.env['val1-secret'];
			//this.editor.setValue(secretData.apiSettings);
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
			this.showTestConnection = true;
		}else{
			document.querySelectorAll('.catalog-form-db-fields input, .catalog-form-db-fields select').forEach(inpt => {
				inpt.removeAttribute('required');
				inpt.removeAttribute('(required)');
			});
			document.querySelectorAll('.catalog-form-secret-group input').forEach(inpt => inpt.setAttribute('required', true));
			this.showTestConnection = false;
		}
	}

	resetForm(showTestConnection = false){
		this.showTestConnection = showTestConnection;
		this.connectionName = '';
		this.dbHost = '';
		this.dbPort = '';
		this.dbName = '';
		this.dbUser = '';
		this.dbEngine = '';
		this.isDbConnEditing = false;
		if(document.querySelector('.first-secret-field')) document.querySelector('.first-secret-field').value = '';
		document.querySelector('.connectio-test-status').style.background = 'rgb(182, 182, 182)';
	}

	showDialog(reset = false, type = null){		
		if(type === 'api') this.markRequiredApiFields(true);
		if(reset) {
			this.isDbConnEditing = false; this.isNewSecret = true, this.resetForm();
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
		this.closeModal.addEventListener('click', () => localResetForm());
		window.addEventListener('click', (e) => e.target === modal ? localResetForm() : '');

		function localResetForm(){
			document.querySelector('.save-secret-btn').style.display = '';
			document.querySelector('.btn-add-secret').disabled = false;
			document.querySelector('input[data="unique-api-name"]').disabled = false;
			self.dataBaseSettingType = null;
			self.modal.style.display = 'none';
			self.showAddSecrete = false;
			self.firstKey = '', self.firstValue = '';
			self.apiBaseUrl = '', self.apiConnName = '';
			self.apiEndpointPath1 = '', self.apiEndpointPathPK1 = '';
			self.paginationStartField1 = '', self.paginationLimitField1 = '';
			self.paginationRecPerPage1 = '', self.apiKeyName = '';
			self.apiKeyValue = '', self.apiTknValue = '';
			self.endpointCounter = 1;
			self.endPointEditorContent = {};
			self.isDbConnEditing = false;
			self.onAPIAuthChange(null);
			document.querySelector('.use-auth-checkbox').checked = false;
			document.querySelector('.use-auth-secret-input').style.display = 'none';
			document.querySelectorAll('input[name="userPagination1"]')[1].click();
			document.querySelectorAll('input[name="dbSettingType"]').forEach(opt => opt.checked = false);
			self.isNewSecret = false;
			self.showTestConnection = false;
			self.markRequiredApiFields(false);
			document.querySelector('.connectio-test-status').style.background = 'rgb(182, 182, 182)';
			
			for(const btn of self.dynamicEndpointsDelButtons)
				btn.click();
			self.dynamicEndpointsDelButtons = [];
		}
	}

	showHideWalletFields = (show) => handleShowHideWalletFields(show);

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

	async testConnection(){
		const btn = document.querySelector('.connectio-test-status');
		btn.parentElement.disabled = true;
		this.checkConnection = 'in-progress';
		const result = await WorkspaceService.testDbConnection({ env: this.getDynamicFields(), dbConfig: this.getDBConfig()}, this.isDbConnEditing);
		btn.style.background = result == true ? 'green' : 'red';
		this.checkConnection = '';
		btn.parentElement.disabled = false;
	}

	async createSecret(){
		let validate = await this.formRef.validate(), dbConfig = null, apiSettings = null, updatingSecret;

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
					dbConfig = this.getDBConfig();
				}
				updatingSecret = this.getUpdatingSecret();
				if(updatingSecret.updatingId && this.isNewSecret)
					return AppTemplate.toast.error(`Secret with name ${this.connectionName.value} already exists`);
				
			}else{
				apiSettings = {
					...this.parseAPICatalogFields(), keyName: this.apiKeyName.value, keyValue: this.apiKeyValue.value, 
					token: this.apiTknValue.value, apiBaseUrl: this.apiBaseUrl.value, apiAuthType: this.apiAuthType
				};
			}
			const connectionName = this.dataBaseSettingType != null ? this.connectionName.value : this.apiConnName.value;
			const result = await WorkspaceService.createSecret({ 
				env: this.getDynamicFields(), dbConfig, apiSettings, connectionName
			});

			if(result === true && this.dataBaseSettingType != null)
				this.updateLeftMenuSecretList({...updatingSecret, showTestConnection: true, db: true });
			else if (apiSettings !== null)
				this.updateLeftMenuSecretList({...apiSettings, name: this.apiConnName.value, api: true });
		}else{
			AppTemplate.toast.error('Please fill all required field');
		}
	}

	getDBConfig = () => ({
		'plugin_name': this.dbEngine.value,
		'connection_url': 'postgresql://{{username}}:{{password}}@{{host}}:{{port}}/{{dbname}}',
		'verify_connection': false,
		'username': this.dbUser.value,
		'password': this.firstValue.value,
		'dbname': this.dbName.value,
		'host': this.dbHost.value,
		'port': this.dbPort.value,
		'connectionName': this.connectionName.value,
		'dbConnectionParams': this.dbConnectionParams.value,
	})

	getOracleDN = async () => {
		if(this.dbHost.value == '' || this.dbPort.value == '' || this.dbName.value == '')
			return AppTemplate.toast.error('Fill <b>Host</b>, <b>Port</b> and <b>Database</b> to field to fetch oracle DN',7000);
		this,this.dbConnectionParams = generateDsnDescriptor(this.dbHost.value,this.dbPort.value,this.dbName.value);
		//document.querySelector('.db-connection-params').disabled = true;

		//this.dbConnectionParams = 'searching';
		//const oracleDN = await WorkspaceService.getOracleDN(this.dbHost.value, this.dbPort.value);
		//if(oracleDN != null) this.dbConnectionParams = `ssl_server_cert_dn=${oracleDN}&ssl_server_dn_match=True&protocol=tcps`;
		//else this.dbConnectionParams = '';

		document.querySelector('.db-connection-params').disabled = false;
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

	updateLeftMenuSecretList(data){
		const { showTestConnection, db, api } = data;
		if(db){
			const { updatingId, updatedSecrets } = data;
			const host = this.dataBaseSettingType == 2 ? 'None' : this.dbHost.value;
			if(updatingId !== null) updatedSecrets[updatingId].host = host;
			if(updatingId === null) updatedSecrets.push({ name: this.connectionName.value, host });
			this.$parent.controller.leftTab.dbSecretsList = updatedSecrets;
		}else if(api){
			const { apiBaseUrl, name } = data;
			const listOfAPiSecrets = this.$parent.controller.leftTab.apiSecretsList.value;
			listOfAPiSecrets.push({ name, host: apiBaseUrl });
			this.$parent.controller.leftTab.apiSecretsList = listOfAPiSecrets;
		}
		this.resetForm(showTestConnection);
	}

	onAPIAuthChange = (type = null) =>  this.apiAuthType = onAPIAuthChange(type);

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

	/** @param { CatalogEndpointType } details */
	addEndpointFields = (details = null) => {
		this.endpointCounter = this.endpointCounter.value + 1;
		handleAddEndpointField(this.endpointCounter.value, this, details);
	}

	parseAPICatalogFields(){

		const endPointsGroup = {
			paginationStartField: [this.paginationStartField1.value],
			paginationLimitField: [this.paginationLimitField1.value],
			paginationRecPerPage: [this.paginationRecPerPage1.value],
			apiEndpointPath: [this.apiEndpointPath1.value],
			apiEndpointPathPK : [this.apiEndpointPathPK1.value],
			apiEndpointDS : [this.apiEndpointDS1.value],
			apiEndpointParams : [this.endPointEditorContent[1] || {}],
		}

		const dynamicFields = this.getDynamicFields();
		const validFieldNames = [
			'apiEndpointPath','apiEndpointPathPK','apiEndpointDS', 'paginationStartField',
			'paginationLimitField','paginationRecPerPage',
		];

		let endpointOrderTrace = 2;
		while(true){
			const x = endpointOrderTrace++;
			const apiPath = dynamicFields[`apiEndpointPath${x}`];
			if(apiPath === null) continue;
			if(apiPath === undefined) break;
			
			for(const field of validFieldNames){
				const fieldValue = dynamicFields[`${field}${x}`] || '';
				endPointsGroup[field].push(fieldValue);
			}
			endPointsGroup['apiEndpointParams'].push(this.endPointEditorContent[x] || {});
		}

		return { 
			apiBaseUrl: this.apiBaseUrl.value, apiKeyName: this.apiKeyName.value,
			apiKeyValue: this.apiKeyValue.value, apiTknValue: this.apiTknValue.value,
			endPointsGroup
		}
	}

	showPaginateEndpoint = () => showHidePaginateEndpoint(1, true);
	hidePaginateEndpoint = () => {
		this.paginationStartField1 = '', this.paginationLimitField1 = '', this.paginationRecPerPage1 = '';
		showHidePaginateEndpoint(1, false);
	}

	markRequiredApiFields = (flag) => {
		if(flag) document.querySelectorAll('.api-required-field').forEach(ipt => ipt.setAttribute('required',true));
		else document.querySelectorAll('.api-required-field').forEach(ipt => ipt.removeAttribute('required'));
	}

}