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
	bucketUrl;
	provider;
	filePattern;

	/** @Prop */
	inConnectors = 1;
	/** @Prop */
	outConnectors = 1;

	/** @Prop */
	formRef;

	/**
	 * @Inject
	 * @Path services/
	 * @type { WorkspaceService }
	 */
	wspaceService;

	/**
	 * The id will be passed when instantiating Bucket dinamically
	 * through the Component.new(type, param) where for para nodeId 
	 * will be passed
	 * */
	stOnRender(nodeId) {
		this.nodeId = nodeId;
	}

	stAfterInit() {
		this.wspaceService.on('load', () => { });

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
		console.log(`RESULT OF VAL IS: `, res);

	}


}