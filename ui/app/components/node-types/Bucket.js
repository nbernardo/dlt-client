import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { WorkSpaceController } from "../../controller/WorkSpaceController.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";

export class Bucket extends ViewComponent {

	isPublic = true;

	/** This is strictly to reference the object in the diagram 
	 * @Prop
	 * */
	nodeId;

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

	/**
	 * @Inject
	 * @Path services/
	 * @type { WorkspaceService }
	 */
	wspaceService;

	/**
	 * @Inject @Path services/
	 * @type { WorkSpaceController } */
	wSpaceController;

	/**
	 * The id will be passed when instantiating Bucket dinamically
	 * through the Component.new(type, param) where for para nodeId 
	 * will be passed
	 * */
	stOnRender({ nodeId, isImport, filePattern, bucketFileSource }){
		this.nodeId = nodeId;
		this.isImport = isImport;
		//this.filePattern = filePattern;
		//this.source = bucketFileSource;
	}

	stAfterInit(){

		this.wspaceService.on('load', () => { });

		const data = WorkSpaceController.getNode(this.nodeId).data;
		data['bucketFileSource'] = 1;

		// When importing, it might take some time for things to be ready, the the subcrib to on change
		// won't be automatically, setupOnChangeListen() will be called explicitly in the WorkSpaceController		
		if([false,undefined].includes(this.isImport)){
			this.setupOnChangeListen();
			console.log(`CALLED ON CHANGE`);
		}else{
			console.log(`--- DIDN'T CALLED ON CHANGE`);
		}

		if(this.isImport){
			// At this point the WorkSpaceController was loaded by WorkSpace component
			// hance no this.wSpaceController.on('load') subscrtiption is needed
			this.wSpaceController.disableNodeFormInputs(this.formWrapClass);
		}

	}

	setupOnChangeListen() {
		
		this.bucketFileSource.onChange((newValue) => {
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


}