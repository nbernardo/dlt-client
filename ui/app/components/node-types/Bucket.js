import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { WorkSpaceController } from "../../controller/WorkSpaceController.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";
import { DataSourceFields, MoreOptionsMenu } from "./util/DataSourceUtil.js";

export class Bucket extends ViewComponent {

	isPublic = false;

	/** This is strictly to reference the object in the diagram 
	 * @Prop */ nodeId;

	label = 'Source Bucket'
	bucketUrl = '';
	provider;
	filePattern;
	bucketFileSource;

	/** @Prop */
	showBucketUrlInput = 1;

	/** @Prop */
	inConnectors = 1;
	/** @Prop */
	outConnectors = 1;

	/** @Prop */ formRef;
	/** @Prop */ isImport = false;
	/** @Prop */ formWrapClass = '_'+UUIDUtil.newId();
	/** @Prop */ showMoreFileOptions = false;
	/** @Prop @type { MoreOptionsMenu } */ moreOptionsRef = null;

	/**
	 * @Inject @Path services/
	 * @type { WorkspaceService } */
	wspaceService;

	/**
	 * @Inject @Path services/
	 * @type { WorkSpaceController } */
	wSpaceController;

	filesFromList = [];

	/** @Prop */ dataSourceFieldsMap = new Map();

	/* The id will be passed when instantiating Bucket dinamically through the
	 * Component.new(type, param) where for para nodeId will be passed  */
	stOnRender({ nodeId, isImport }){
		this.nodeId = nodeId;
		this.isImport = isImport;
	}

	async stAfterInit(){

		const data = WorkSpaceController.getNode(this.nodeId).data;
		data['bucketFileSource'] = 1;
		this.filesFromList = await this.wspaceService.listFiles();

		// When importing, it might take some time for things to be ready, the the subcrib to on change
		// won't be automatically, setupOnChangeListen() will be called explicitly in the WorkSpaceController		
		if([false,undefined].includes(this.isImport))
			this.setupOnChangeListen();

		if(this.isImport){
			// At this point the WorkSpaceController was loaded by WorkSpace component
			// hance no this.wSpaceController.on('load') subscrtiption is needed
			this.wSpaceController.disableNodeFormInputs(this.formWrapClass);
		}

	}

	setupOnChangeListen() {
		
		this.bucketFileSource.onChange(async (newValue) => {
			
			if(this.showBucketUrlInput === 1){

				const selectdFile = newValue.trim();
				this.showMoreFileOptions = 'searching';
				if(!this.dataSourceFieldsMap.has(selectdFile)){
					const fields = await this.wspaceService.getCsvFileFields(newValue.trim());
					if(fields != null) {
						// API Response will be something like Index(['ID', 'Name', 'Age', 'Country'], dtype='object')
						// hence bellow we're clearing things up so to have an array with the proper field names
						const fieldList = fields.split('[')[1].split(']')[0].replace(/\'|\s/g,'').split(',')
							.map((name, id) => ({ name, id, type: 'string' }));

						this.dataSourceFieldsMap.set(selectdFile, fieldList);
					}
				}

				if(newValue.trim() != "") this.showMoreFileOptions = true;
				else this.showMoreFileOptions = false;
				return;
			}

			this.showBucketUrlInput = newValue;
			const data = WorkSpaceController.getNode(this.nodeId).data;
			data['bucketFileSource'] = newValue;
		});

		this.bucketUrl.onChange((newValue) => {
			const data = WorkSpaceController.getNode(this.nodeId).data;
			data['bucketUrl'] = newValue;
		});

		this.filePattern.onChange((newValue) => {
			const data = WorkSpaceController.getNode(this.nodeId).data;
			data['filePattern'] = newValue;
		});

		this.provider.onChange((newValue) => {
			const data = WorkSpaceController.getNode(this.nodeId).data;
			data['provider'] = newValue;
		});
	}

	getMyData() {
		const data = WorkSpaceController.getNode(this.nodeId);
		WorkSpaceController.getNode(this.nodeId).html = '';
		console.log(data.data);
	}

	validate() {
		const res = this.formRef.validate();
	}

	setMoreOptionsMenu(e, containerId){
		const filesList = this.dataSourceFieldsMap.get(this.bucketFileSource.value);
		if(this.moreOptionsRef !== null)
			return this.moreOptionsRef.handleShowPopup(e, containerId);
		
		this.fieldListRender(containerId, filesList);
		this.moreOptionsRef = (new MoreOptionsMenu).handleShowPopup(e, containerId);

	}

	addFieldInDataSource(fieldsContainerId){
		const existingFields = this.dataSourceFieldsMap.get(this.bucketFileSource.value);
		existingFields.push({ name: 'Dbl click to edit', id: existingFields.length, type: 'string', new: true });
		const popupId = fieldsContainerId.split('-')[1];
		this.fieldListRender(popupId, existingFields);
		
	}

	showTypeMenu(fieldId, clickedIcon, popupId){
		const filesList = this.dataSourceFieldsMap.get(this.bucketFileSource.value);
		(new DataSourceFields).showTypeMenu(fieldId, clickedIcon, filesList, popupId, this.fieldListRender, this);
	}

	hidePopup(popupId){
		document.querySelector('.popup-right-sede-'+popupId).style.display = 'none';
	}

	fieldListRender(popupId, filesList, _obj, fieldId, newType) {	
		
		const field = filesList.find(f => f.id === fieldId);
        if (field) field.type = newType;
        
        let container = document.querySelector('.fields-'+popupId);
		if(!container) container = document.querySelector('.fields-_'+popupId);
        container.innerHTML = '';

        const obj = _obj || this;
        filesList?.forEach(field => {
            const div = document.createElement('div');
			const containerId = popupId.replace('_',''); // Replace underscore in case it exists
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
		container.scrollTop = container.style.height.replace('px','');
    }

	confirmFieldName(e) {
		if (e.key === 'Enter') {
			e.preventDefault();
			e.target.blur();
			e.target.style.color = 'black';
			e.target.style.fontWeight = 'bold';
		}
	}

}


