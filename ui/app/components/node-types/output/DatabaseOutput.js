import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { STForm } from "../../../../@still/component/type/ComponentType.js";
import { WorkSpaceController } from "../../../controller/WorkSpaceController.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { Bucket } from "../Bucket.js";
import { NodeTypeInterface } from "../mixin/NodeTypeInterface.js";

/** @implements { NodeTypeInterface } */
export class DatabaseOutput extends ViewComponent {

	isPublic = true;

	label = 'Database Output';
	databaseEngines = [
		{ name: 'MySQL', dialect: 'mysql' },
		{ name: 'Postgress', dialect: 'postgresql' },
		{ name: 'Oracle', dialect: 'oracle' },
		{ name: 'SQL Server', dialect: 'mssql' }
	];

	/** @Prop */ inConnectors = 1;
	/** @Prop */ nodeId;
	/** @Prop */ dbInputCounter = 1;
	/** @Prop */ isConnected = false;

	selectedSecretTableList = [];
	database = 'Not selected';
	selectedDbEngine = 'Not selected';
	selectedSecret;
	secretList = [];
	hostName = 'Not selected';

	/** @Prop */ isImport = false;
	/** @Prop @type { STForm } */ anotherForm;
	/** @Prop */ showLoading = false;
	/** @Prop */ importFields;

	//This is only used in case the source 
	// is not a Database (e.g. Bucket, InputAPI)
	/** @Prop */ tableName; 

	/**
	 * @Inject @Path services/
	 * @type { WorkSpaceController } */
	wSpaceController;

	/** The id will be passed when instantiating SqlDBComponent dinamically through
	 * the Component.new(type, param) where for para nodeId will be passed */
	stOnRender(data){				
		const { nodeId, isImport, database, dbengine, outDBconnectionName, databaseName, host } = data;
		this.nodeId = nodeId;
		this.isImport = isImport;
		this.importFields = { database, dbengine, outDBconnectionName, databaseName, host, dbengine };
		if(data?.host) this.importFields.host = data.host;
	}

	async stAfterInit(){
		this.tableName = null;
		await this.getDBSecrets();
		if(this.isImport === true){	
			this.selectedDbEngine = this.importFields.dbengine;
			this.selectedSecret = this.importFields.outDBconnectionName;
			this.database = this.importFields.databaseName;
			this.hostName = this.importFields.host || 'None';
			document.querySelector(`.${this.cmpInternalId} select[data-dropdown]`).disabled = true;
		}
		this.setupOnChangeListen(); 
	}

	setupOnChangeListen(){
		this.selectedSecret.onChange(async secretName => {
			this.showLoading = true;
			let database = '', dbengine = '', host = '';
			if(secretName != ''){
				const data = await WorkspaceService.getConnectionDetails(secretName);
				const detail = data['secret_details'];
				database = detail?.database, dbengine = detail?.dbengine, host = detail?.host;
				WorkSpaceController.getNode(this.nodeId).data['outDBconnectionName'] = secretName;
				WorkSpaceController.getNode(this.nodeId).data['databaseName'] = database;
				WorkSpaceController.getNode(this.nodeId).data['host'] = host;
				WorkSpaceController.getNode(this.nodeId).data['dbengine'] = dbengine;
			}
			this.database = database, this.selectedDbEngine = dbengine, this.hostName = host;
			this.showLoading = false;
			this.updateConnection();
		});
	}

	async getDBSecrets(){
		this.secretList = (await WorkspaceService.listSecrets(1)).filter(itm => itm.host != 'None');
	}

	updateConnection(){
		const connectionName = this.tableName || this.selectedSecret.value;
		console.log(`NEW DESTINATION NAME ID: `, connectionName);
		
		if(this.isConnected){
			if(connectionName === '')
				delete this.wSpaceController.pipelineDestinationTrace.sql[this.cmpInternalId];
			else
				this.wSpaceController.pipelineDestinationTrace.sql[this.cmpInternalId] = connectionName;
		}
	}

	onInputConnection({data: { sourceNode }, type}){
		if(type == Bucket.name){
			/** @type { Bucket } */
			const sourceNodeObj = sourceNode;
			this.tableName = sourceNodeObj.filePattern.value;

			/** This will be triggered in case the source connection is
			 *  bucket or file system and the file name was changed */
			sourceNodeObj.filePattern.onChange(value => {
				const table = String(value).split('.');
				this.tableName = table.slice(0, table.length - 1).join('');
				this.updateConnection();
			});
		}
		this.isConnected = true;
		this.updateConnection();
	}

	stOnUnload(){
		delete this.wSpaceController.pipelineDestinationTrace.sql[this.cmpInternalId];
	}

	onConectionDelete(sourceType){
		if(sourceType === Bucket.name)
			this.tableName = null;
	}
	
}