import { STForm } from "../../../../@still/component/type/ComponentType.js";
import { WorkSpaceController } from "../../../controller/WorkSpaceController.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { AbstractNode } from "../abstract/AbstractNode.js";
import { InputAPI } from "../api/InputAPI.js";
import { Bucket } from "../Bucket.js";
import { NodeTypeInterface } from "../mixin/NodeTypeInterface.js";
import { SqlDBComponent } from "../SqlDBComponent.js";
import { Transformation } from "../Transformation.js";
import { InputConnectionType } from "../types/InputConnectionType.js";
import { databaseIcons, databaseEnginesList } from "../util/databaseUtil.js";

/** @implements { NodeTypeInterface } */
export class DatabaseOutput extends AbstractNode {

	isPublic = true;

	label = 'Database Output';
	databaseEngines = databaseEnginesList;

	/** @Prop */ inConnectors = 1;
	/** @Prop */ nodeId;
	/** @Prop */ dbInputCounter = 1;
	/** @Prop */ isConnected = false;
	/** @Prop */ dbIcon = databaseIcons.output;

	selectedSecretTableList = [];
	database = 'Not selected';
	selectedDbEngine = 'Not selected';
	selectedSecret;
	secretList = [];
	hostName = 'Not selected';
	nodeCount = '';

	/** @Prop */ isImport = false;
	/** @Prop @type { STForm } */ anotherForm;
	/** @Prop */ showLoading = false;
	/** @Prop */ importFields;
	/** @Prop */ secretedSecretTrace = null;
	/** @Prop */ aiGenerated = null;
	/** @Prop */ sourceNode = null;

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
		const { nodeId, isImport, database, dbengine, outDBconnectionName, connectionName, databaseName, host, aiGenerated } = data;
		
		this.nodeId = nodeId;
		this.isImport = isImport;
		this.aiGenerated = aiGenerated;
		this.importFields = { database, dbengine, outDBconnectionName, databaseName, host, dbengine, connectionName };
		if(data?.host) this.importFields.host = data.host;
	}

	async stAfterInit(){
		this.tableName = null;
		this.selectedSecretTableList = [];
		await this.getDBSecrets();
		if(this.isImport === true){	
			this.selectedDbEngine = this.importFields.dbengine;
			this.setDBIcon(this.selectedDbEngine.value);
			this.selectedSecret = this.importFields.outDBconnectionName;
			this.database = this.importFields.databaseName;
			this.hostName = this.importFields.host || 'None';
			document.querySelector(`.${this.cmpInternalId} select[data-dropdown]`).disabled = this.wSpaceController.shouldDisableNodeFormInputs;
			WorkSpaceController.getNode(this.nodeId).data['outDBconnectionName'] = this.importFields.outDBconnectionName;
			WorkSpaceController.getNode(this.nodeId).data['databaseName'] = this.importFields.databaseName;
			WorkSpaceController.getNode(this.nodeId).data['host'] = this.importFields.host;
			WorkSpaceController.getNode(this.nodeId).data['dbengine'] = this.importFields.dbengine;
		}
		this.setupOnChangeListen();
		if(this.aiGenerated) this.handleAiGenerated();
	}

	handleAiGenerated(){
		this.selectedSecret = this.importFields.connectionName || '';
	}

	setupOnChangeListen(){
		this.selectedSecret.onChange(async secretName => {
			// To prevent running through the bellow steps in case the secret is the same
			if(this.secretedSecretTrace == secretName) return;

			this.secretedSecretTrace = secretName
			this.showLoading = true;
			let database = '', dbengine = '', host = '';
			if(secretName != ''){
				const data = await WorkspaceService.getConnectionDetails(secretName);
				if('secret_details' in (data || {})){
					const detail = data['secret_details'];
					database = detail?.database, dbengine = detail?.dbengine, host = detail?.host;
					WorkSpaceController.getNode(this.nodeId).data['outDBconnectionName'] = secretName;
					WorkSpaceController.getNode(this.nodeId).data['databaseName'] = database;
					WorkSpaceController.getNode(this.nodeId).data['host'] = host;
					WorkSpaceController.getNode(this.nodeId).data['dbengine'] = dbengine;
				}
			}
			this.database = database, this.selectedDbEngine = dbengine, this.hostName = host;
			this.showLoading = false;
			this.setDBIcon(dbengine);
			this.updateConnection();
		});
	}

	async getDBSecrets(){
		this.secretList = (await WorkspaceService.listSecrets(1)).filter(itm => itm.host != 'None' && itm.bucket == 'no');
	}

	async reloadMe(){
		this.showLoading = true;
		await this.getDBSecrets();
		this.selectedSecret = '';
		this.showLoading = false;
	}

	updateConnection(){
		let connectionName = this.tableName !== null ? this.tableName : this.selectedSecret.value;
		if(this.isConnected){
						
			if(connectionName === '')
				delete this.wSpaceController.pipelineDestinationTrace.sql[this.cmpInternalId];
			else{
				if(this.sourceNode?.getName() === SqlDBComponent.name){
					connectionName = this.sourceNode.selectedSecret.value;
				}

				this.wSpaceController.pipelineDestinationTrace.sql[this.cmpInternalId] = connectionName;
			}
		}
	}

	/** @param {InputConnectionType<{}>} param0  */
	onInputConnection({data, type}){
		
		DatabaseOutput.handleInputConnection(this, data, type);
		
		const sourceNode = data?.sourceNode; 
		this.sourceNode = data?.sourceNode;
		if((type == Bucket.name || type == Transformation.name) && sourceNode){
			if(sourceNode.filePattern){
				/** @type { Bucket } */
				const sourceNodeObj = sourceNode;
				this.tableName = sourceNodeObj.filePattern.value;
	
				/** This will be triggered in case the source connection is
				 *  bucket or file system and the file name was changed */
				sourceNodeObj.filePattern.onChange(value => {
					const table = String(value).split('.');
					this.tableName = table.slice(0, table.length - 1).join('').replace('-','_');
					this.updateConnection();
				});
			}
		}

		if(type === InputAPI.name) this.tableName = 'API';

		this.isConnected = true;
		this.updateConnection();
	}

	stOnUnload(){
		if(!this.isImport)
			delete this.wSpaceController.pipelineDestinationTrace.sql[this.cmpInternalId];
	}

	onConectionDelete(sourceType){
		if(sourceType === Bucket.name || sourceType === InputAPI.name)
			this.tableName = null;
		this.nodeCount = '';
	}

	setDBIcon = (db) => 
		document.querySelector(`.${this.cmpInternalId}`)
			.querySelector('.database-icon').src = databaseIcons[db == '' ? 'output' : db];
	
}