import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";

export class Bucket extends ViewComponent {

	isPublic = true;

	/** This is strictly to reference the object in the diagram 
	 * @Prop
	 * */
	nodeId;

	basketUrl;

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
	}



}