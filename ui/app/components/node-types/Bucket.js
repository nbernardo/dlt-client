import { STForm } from "../../../@still/component/type/ComponentType.js";
import { WorkSpaceController } from "../../controller/WorkSpaceController.js";
import { UserService } from "../../services/UserService.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";
import { InputDropdown } from "../../util/InputDropdownUtil.js";
import { AbstractNode } from "./abstract/AbstractNode.js";
import { NodeTypeInterface } from "./mixin/NodeTypeInterface.js";
import { Transformation } from "./Transformation.js";
import { InputConnectionType } from "./types/InputConnectionType.js";
import { DataSourceFields, MoreOptionsMenu } from "./util/DataSourceUtil.js";

/** @implements { NodeTypeInterface } */
export class Bucket extends AbstractNode {

	isPublic = false;

	/** This is strictly to reference the object in the diagram 
	 * @Prop */ nodeId;

	label = 'Source Bucket'
	bucketUrl = '';
	provider;
	filePattern;
	bucketFilePattern;
	bucketFileSource;
	selectedFilePattern;
	sourcePrimaryKey;
	nodeCount = '';
	secretsList = '';

	/** @Prop */ showBucketUrlInput = 1;
	/** @Prop */ inConnectors = 1;
	/** @Prop */ outConnectors = 1;
	/** @Prop */ aiGenerated;
	/** @Prop */ importFields;

	/** @Prop @type { STForm } */ formRef;
	/** @Prop */ isImport = false;
	/** @Prop */ formWrapClass = '_' + UUIDUtil.newId();
	/** @Prop */ showMoreFileOptions = false;
	/** @Prop @type { MoreOptionsMenu } */ moreOptionsRef = null;
	/** @Prop @type { Transformation } */ transformationStep = false;

	/** @Prop */ showLoading = false;
	/**
	 * @Inject @Path services/
	 * @type { WorkspaceService } */
	wspaceService;

	/**
	 * @Inject @Path services/
	 * @type { WorkSpaceController } */
	wSpaceController;

	filesFromList = [];
	bucketObjects = [];
	selectedSecret;
	
	/** @Prop */ bucketObjectsSchemaMap;

	/* The id will be passed when instantiating Bucket dinamically through the
	 * Component.new(type, param) where for para nodeId will be passed  */
	stOnRender(data) {
		const { 
			nodeId, isImport, bucketUrl, filePattern, primaryKey, 
			bucketFileSource, aiGenerated, url, file, connectionName
		} = data;
		this.nodeId = nodeId;
		this.isImport = isImport;
		if(isImport) this.showLoading = true;
		if(bucketUrl) this.bucketUrl = bucketUrl;
		if(filePattern) this.filePattern = filePattern;
		if(primaryKey) this.sourcePrimaryKey = primaryKey;
		if(bucketFileSource) this.bucketFileSource = bucketFileSource;

		this.importFields = { bucketUrl, url, filePattern, file, connectionName };
		this.aiGenerated = aiGenerated;

		setTimeout(() => this.showMoreFileOptions = true, 5000);
	}

	async getFilesList(){
		const result = await this.wspaceService.listFiles();
		this.filesFromList = (result || []).map(file => ({ ...file, name: `${file.name.split('.').slice(0,-1)}*.${file.type}`, file: file.name }));
	}

	async reloadMe(){
		this.showLoading = true;
		await this.getFilesList();
		this.showLoading = false;
	}

	async stAfterInit() {
		const data = WorkSpaceController.getNode(this.nodeId).data;
		data['bucketFileSource'] = 1;
		data['namespace'] = await UserService.getNamespace();
		await this.getFilesList();
		// When importing, it might take some time for things to be ready, the the subcrib to on change
		// won't be automatically, setupOnChangeListen() will be called explicitly in the WorkSpaceController		
		if ([false, undefined].includes(this.isImport) || this.aiGenerated) this.setupOnChangeListen();

		this.secretsList = (await WorkspaceService.listSecrets(3)).filter(itm => itm.host != 'None');

		if(this.aiGenerated){
			const bucketUrl = this.importFields.url ? this.importFields.url : this.importFields.bucketUrl;
			const flPattern = this.importFields.file ? this.importFields.file : this.importFields.filePattern;
			this.filePattern = flPattern || '';
			this.bucketUrl = bucketUrl || '';

			if(this.bucketUrl.value.length > 0) this.bucketFileSource = 2;
		}

		// At this point the WorkSpaceController was loaded by WorkSpace component
		// hance no this.wSpaceController.on('load') subscrtiption is needed
		if (this.isImport) {
			// This is mainly because WorkSpaceController will setup reactive notification from source component to 
			// terget component if connection is being created, regular targets of Backet are Transformation and 
			// DuckDBOutput, in case isImport == true, this event is emitted when data source/files are listed
			this.notifyReadiness();
			this.selectedSecret = this.importFields.connectionName;
			if(this.importFields.connectionName){
				const { files, schemas } = await WorkspaceService.getBucketObjects(this.importFields.connectionName);
				this.bucketObjects = files, this.bucketObjectsSchemaMap = schemas;
			}

			this.selectedFilePattern = this.filePattern.value;
			if(this.bucketFileSource.value === '2'){
				this.bucketFileSource = 1;
				this.setupOnChangeListen();
				setTimeout(() => {
					this.filePattern = this.importFields.filePattern;
					this.bucketFileSource = 2;
					this.wSpaceController.disableNodeFormInputs(this.formWrapClass);
					this.showLoading = false;
				}, 100);
			}else{
				this.wSpaceController.disableNodeFormInputs(this.formWrapClass);
				this.showLoading = false;
			}
			data['filePattern'] = this.filePattern.value;
			data['readFileType'] = this.filePattern.value.split('.').slice(-1);
			this.bucketUrl = this.bucketFileSource.value;
			this.selectedFilePattern = this.filePattern.value;

		}
	}

	setupOnChangeListen() {
		const mainContnr = document.querySelector('.'+this.cmpInternalId);
		this.bucketFileSource.onChange(async (newValue) => {
			this.showBucketUrlInput = Number(newValue), this.selectedSecret = '';
			WorkSpaceController.isS3AuthTemplate= false;
			delete WorkSpaceController.getNode(this.nodeId).data['connectionName'];
			if(this.showBucketUrlInput == 2){
				mainContnr?.querySelector('.input-file-bucket')?.removeAttribute('(required)');
				mainContnr?.querySelectorAll('.input-file-bucket1')?.forEach(elm => elm.setAttribute('required', true));
				WorkSpaceController.isS3AuthTemplate = true;
				this.transformationStep.updateTransformationRows([{ name: 'No table'}]);
			}else{
				mainContnr?.querySelector('.input-file-bucket')?.setAttribute('required',true);
				mainContnr?.querySelectorAll('.input-file-bucket1')?.forEach(elm => elm.removeAttribute('required'));
				this.transformationStep.updateTransformationRows(this.filesFromList.value);
			}
			this.setNodeData('bucketFileSource', newValue);
		});

		this.bucketUrl.onChange((newValue) => this.setNodeData('bucketUrl', newValue));

		this.selectedSecret.onChange(async value => {
			if(value.trim() == '' && this.bucketFileSource.value == 2)
				return this.transformationStep.updateTransformationRows([{ name: 'No table'}]);
			if(this.bucketFileSource.value == 2){
				this.showMoreFileOptions = true;
				const { files, schemas } = await WorkspaceService.getBucketObjects(value);
				this.bucketObjects = files;
				setTimeout(() => this.showMoreFileOptions = false, 100);
				this.setNodeData('selectedSecret', value);
				this.transformationStep.updateTransformationRows(this.bucketObjects.value, schemas);
			}
		});

		this.selectedFilePattern.onChange(async (newValue) => {
			const selectdFile = newValue.trim();
			if (selectdFile == '') return;
			
			this.showMoreFileOptions = 'searching';
			await this.wspaceService.handleCsvSourceFields(selectdFile);

			const fileType = selectdFile.toLowerCase().split('.').slice(-1)[0];
			this.setNodeData('readFileType', fileType)

			if (newValue.trim() != "") this.showMoreFileOptions = true;
			else this.showMoreFileOptions = false;

			this.filePattern = selectdFile;
			this.bucketUrl = 'user_folder';
		});

		this.filePattern.onChange(async (v) => this.handleFilePattern(v));
		this.bucketFilePattern.onChange(async (v) => {
			await this.handleFilePattern(v, true); this.setNodeData('connectionName', this.selectedSecret.value);
		});
		this.provider.onChange((v) => this.setNodeData('provider', v));
		this.sourcePrimaryKey.onChange(v => this.setNodeData('primaryKey', v));
	}
	
	/** @Prop */ previousFilePattern = '';
	async handleFilePattern(newValue, fromCloud = false){
		if(this.previousFilePattern.trim() === newValue.trim()) return;
		this.previousFilePattern = newValue;

		if(fromCloud) {
			let dataSource = await WorkspaceService.getBucketObjectFields(this.selectedSecret.value, newValue);
			dataSource = dataSource.data;
			
			InputDropdown.new({ 
				inputSelector: '.sourceBucketPrimaryKey', dataSource,
				boundComponent: this, componentFieldName: null
			});
		}

		if (this.moreOptionsRef !== null) this.moreOptionsRef.popup.style.display = 'none';
		this.setNodeData('filePattern', newValue);
		this.setNodeData('bucketUrl', null);
		this.setNodeData('readFileType', newValue.toLowerCase().split('.').slice(-1)[0]);
	}

	setMoreOptionsMenu(e, containerId) {
		
		const filesList = this.wspaceService.getCsvDataSourceFields(this.filePattern.value);
		this.fieldListRender(containerId, filesList);
		this.moreOptionsRef = (new MoreOptionsMenu).handleShowPopup(e, containerId);
	}

	addFieldInDataSource(fieldsContainerId) {
		const existingFields = this.wspaceService.getCsvDataSourceFields(this.filePattern.value);
		existingFields.push({ name: 'Dbl click to edit', id: existingFields.length, type: 'string', new: true });
		const popupId = fieldsContainerId.split('-')[1];
		this.fieldListRender(popupId, existingFields);

	}

	showTypeMenu(fieldId, clickedIcon, popupId) {
		const filesList = this.dataSourceFieldsMap.get(this.filePattern.value);
		(new DataSourceFields).showTypeMenu(fieldId, clickedIcon, filesList, popupId, this.fieldListRender, this);
	}

	hidePopup(popupId) {
		document.querySelector('.popup-right-sede-' + popupId).style.display = 'none';
	}

	fieldListRender(popupId, filesList, _obj, fieldId, newType) {

		const field = filesList.find(f => f.id === fieldId);
		if (field) field.type = newType;

		let container = document.querySelector('.fields-' + popupId);
		if (!container) container = document.querySelector('.fields-_' + popupId);
		container.innerHTML = '';

		const obj = _obj || this;
		filesList?.forEach(field => {
			const div = document.createElement('div');
			const containerId = popupId.replace('_', ''); // Replace underscore in case it exists
			div.className = 'field';
			// bellow inner. is being used on the events (onclick, onkeypress) because Backed is embeded
			// components and the events are inside itself, if it was not embeded then it would be self.
			div.innerHTML = obj.parseEvents(
				`<div class="icon ${field.type}" onclick="inner.showTypeMenu(${field.id}, this, '${containerId}')">
                    ${DataSourceFields.dataTypes[field.type].icon}
                </div>
                <span class="name" onkeypress="inner.confirmFieldName(event)"
					  ${field.new ? 'contenteditable="true" style="color: #a2a0a0;"' : ''}>${field.name}</span>`
			);
			container.appendChild(div);
		});
		container.scrollTop = container.style.height.replace('px', '');
	}

	confirmFieldName(e) {
		if (e.key === 'Enter') {
			e.preventDefault();
			e.target.blur(), e.target.style.color = 'black', e.target.style.fontWeight = 'bold';
		}
	}

	onOutputConnection() {
		Bucket.handleOutputConnection(this);
		return {
			tables: this.filesFromList.value,
			sourceNode: this,
			nodeCount: this.nodeCount.value
		};
	}

	/** @param { InputConnectionType<{}> } param0 */
	onInputConnection({ type, data }){
		Bucket.handleInputConnection(this, data, type);
	}

}


