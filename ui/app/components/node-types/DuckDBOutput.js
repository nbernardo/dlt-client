import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { WorkSpaceController } from "../../controller/WorkSpaceController.js";

export class DuckDBOutput extends ViewComponent {

	isPublic = true;

	/** @Prop */
	nodeId;

	database;
	tableName;
	label = 'Duckdb Output';

	/** @Prop */
	inConnectors = 1;
	/** @Prop */
	outConnectors = 0;

	/** @Prop */ isImport = false;
	/** @Prop */ formWrapClass = '_'+UUIDUtil.newId();

	//Bellow property is mapped to the
	//form To allow validation check
	/** @Prop */
	formRef;

	/**
	 * @Inject @Path services/
	 * @type { WorkSpaceController } */
	wSpaceController;

	/**
	 * The id will be passed when instantiating DuckDBOutput dinamically
	 * through the Component.new(type, param) where for para nodeId 
	 * will be passed
	 * */
	stOnRender({ nodeId, isImport }){
		this.nodeId = nodeId;
		this.isImport = isImport;
	}

	stAfterInit(){

		// When importing, it might take some time for things to be ready, the the subcrib to on change
		// won't be automatically, setupOnChangeListen() will be called explicitly in the WorkSpaceController
		if(this.isImport !== false){
			this.setupOnChangeListen();
		}

		if(this.isImport === true){	
			// At this point the WorkSpaceController was loaded by WorkSpace component
			// hance no this.wSpaceController.on('load') subscrtiption is needed
			this.wSpaceController.disableNodeFormInputs(this.formWrapClass);
		}

	}

	setupOnChangeListen(){

		this.database.onChange((newValue) => {
			const data = WorkSpaceController.getNode(this.nodeId).data;
			data['database'] = newValue;
		});

		this.tableName.onChange((newValue) => {
			const data = WorkSpaceController.getNode(this.nodeId).data;
			data['tableName'] = newValue;
		});

	}

}