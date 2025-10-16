import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { STForm } from "../../../@still/component/type/ComponentType.js";
import { Components } from "../../../@still/setup/components.js";
import { WorkSpaceController } from "../../controller/WorkSpaceController.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";
import { NodeTypeInterface } from "./mixin/NodeTypeInterface.js";
import { DataSourceFields, MoreOptionsMenu } from "./util/DataSourceUtil.js";

/** @implements { NodeTypeInterface } */
export class Bucket extends ViewComponent {

	isPublic = false;

	/** This is strictly to reference the object in the diagram 
	 * @Prop */ nodeId;

	label = 'Source Bucket'
	bucketUrl = '';
	provider;
	filePattern;
	bucketFileSource;
	selectedFilePattern;
	sourcePrimaryKey;

	/** @Prop */
	showBucketUrlInput = 1;

	/** @Prop */
	inConnectors = 1;
	/** @Prop */
	outConnectors = 1;

	/** @Prop @type { STForm } */ formRef;
	/** @Prop */ isImport = false;
	/** @Prop */ formWrapClass = '_' + UUIDUtil.newId();
	/** @Prop */ showMoreFileOptions = false;
	/** @Prop @type { MoreOptionsMenu } */ moreOptionsRef = null;

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

	/* The id will be passed when instantiating Bucket dinamically through the
	 * Component.new(type, param) where for para nodeId will be passed  */
	stOnRender({ nodeId, isImport, bucketUrl, filePattern, primaryKey, bucketFileSource }) {
		this.nodeId = nodeId;
		this.isImport = isImport;
		if(isImport) this.showLoading = true;
		if(bucketUrl) this.bucketUrl = bucketUrl;
		if(filePattern) this.filePattern = filePattern;
		if(primaryKey) this.sourcePrimaryKey = primaryKey;
		if(bucketFileSource) this.bucketFileSource = bucketFileSource;
	}

	async stAfterInit() {

		const data = WorkSpaceController.getNode(this.nodeId).data;
		data['bucketFileSource'] = 1;
		const result = await this.wspaceService.listFiles();
		this.filesFromList = (result || []).map(file => ({ ...file, name: `${file.name.split('.').slice(0,-1)}*.${file.type}`, file: file.name }));
		
		if(this.isImport){
			// This is mainly because WorkSpaceController will setup reactive notification from source component to 
			// terget component if connection is being created, regular targets of Backet are Transformation and 
			// DuckDBOutput, in case isImport == true, this event is emitted when data source/files are listed
			this.notifyReadiness();
		}

		// When importing, it might take some time for things to be ready, the the subcrib to on change
		// won't be automatically, setupOnChangeListen() will be called explicitly in the WorkSpaceController		
		if ([false, undefined].includes(this.isImport))
			this.setupOnChangeListen();

		// At this point the WorkSpaceController was loaded by WorkSpace component
		// hance no this.wSpaceController.on('load') subscrtiption is needed
		if (this.isImport) {
			this.selectedFilePattern = this.filePattern.value;
			if(this.bucketFileSource.value === '2'){
				this.bucketFileSource = 1;
				this.setupOnChangeListen();
				setTimeout(() => {
					this.bucketFileSource = 2;
					this.wSpaceController.disableNodeFormInputs(this.formWrapClass);
					this.showLoading = false;
				}, 100);
			}else{
				this.wSpaceController.disableNodeFormInputs(this.formWrapClass);
				this.showLoading = false;
			}
			data['filePattern'] = this.filePattern.value;
		}
		

	}

	setupOnChangeListen() {
		const mainContnr = document.querySelector('.'+this.cmpInternalId);
		this.bucketFileSource.onChange(async (newValue) => {
			this.showBucketUrlInput = Number(newValue);
			if(this.showBucketUrlInput == 2){
				mainContnr.querySelector('.input-file-bucket').removeAttribute('(required)');
			}else{
				mainContnr.querySelector('.input-file-bucket').setAttribute('(required)',true);
			}
			this.setNodeData('bucketFileSource', newValue);
		});

		this.bucketUrl.onChange((newValue) => this.setNodeData('bucketUrl', newValue));

		this.selectedFilePattern.onChange(async (newValue) => {
			const selectdFile = newValue.trim();
			if (selectdFile == '') return;

			this.showMoreFileOptions = 'searching';
			await this.wspaceService.handleCsvSourceFields(selectdFile);

			if (newValue.trim() != "") this.showMoreFileOptions = true;
			else this.showMoreFileOptions = false;

			this.filePattern = selectdFile;
			this.bucketUrl = 'user_folder';
		});

		this.filePattern.onChange(async (newValue) => this.setNodeData('filePattern', newValue));

		this.provider.onChange((newValue) => {
			const data = WorkSpaceController.getNode(this.nodeId).data;
			data['provider'] = newValue;
		});

		this.sourcePrimaryKey.onChange(newValue => this.setNodeData('primaryKey', newValue));
	}

	setNodeData = (field, value) => WorkSpaceController.getNode(this.nodeId).data[field] = value;
	
	getMyData() {
		const data = WorkSpaceController.getNode(this.nodeId);
		WorkSpaceController.getNode(this.nodeId).html = '';
		console.log(data.data);
	}

	setMoreOptionsMenu(e, containerId) {
		const filesList = this.wspaceService.getCsvDataSourceFields(this.filePattern.value);
		if (this.moreOptionsRef !== null)
			return this.moreOptionsRef.handleShowPopup(e, containerId);

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
			e.target.blur();
			e.target.style.color = 'black';
			e.target.style.fontWeight = 'bold';
		}
	}

	onOutputConnection() {
		return this.filesFromList.value;
	}

	notifyReadiness = () => 
		Components.emitAction(`nodeReady${this.cmpInternalId}`);
}


