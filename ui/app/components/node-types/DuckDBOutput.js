import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { WorkSpaceController } from "../../controller/WorkSpaceController.js";
import { NodeTypeInterface } from "./mixin/NodeTypeInterface.js";
import { InputConnectionType } from "./types/InputConnectionType.js";
import { NodeUtil } from "./util/nodeUtil.js";

/** @implements { NodeTypeInterface } */
export class DuckDBOutput extends ViewComponent {

	isPublic = true;

	/** @Prop */ nodeId;
	/** @Prop */ aiGenerated;
	/** @Prop */ aiGenerated;
	/** @Prop */ importFields;

	database;
	tableName;
	label = 'Duckdb Output';
	nodeCount = '';

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
	stOnRender(data){
		const { nodeId, isImport, aiGenerated, database, table } = data;
		
		this.aiGenerated = aiGenerated;
		this.nodeId = nodeId;
		this.isImport = isImport;
		this.importFields = { database, table };
	}

	stAfterInit(){		
		// When importing, it might take some time for things to be ready, the the subcrib to on change
		// won't be automatically, setupOnChangeListen() will be called explicitly in the WorkSpaceController
		if(this.isImport === false){
			this.setupOnChangeListen();
		}

		if(this.aiGenerated){
			const { database, table } = this.importFields;
			this.database = (database || ''), this.tableName = (table || '');
		}

		if(this.isImport === true){	
			// At this point the WorkSpaceController was loaded by WorkSpace component
			// hance no this.wSpaceController.on('load') subscrtiption is needed
			const data = WorkSpaceController.getNode(this.nodeId).data;
			this.wSpaceController.disableNodeFormInputs(this.formWrapClass);
			data['database'] = this.database.value;
			data['tableName'] = this.tableName.value;
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

	/** @param {InputConnectionType<{}>} param0  */
	onInputConnection({data, type}){
		NodeUtil.handleInputConnection(this, data, type);
	}

	onConectionDelete(){
		this.nodeCount = '';
	}

}