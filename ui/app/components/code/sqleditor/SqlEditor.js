import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { ListState, State } from "../../../../@still/component/type/ComponentType.js";
import { UUIDUtil } from "../../../../@still/util/UUIDUtil.js";
import { AIResponseLinterUtil } from "../../agent/AIResponseLinterUtil.js";
import { Grid } from "../../dataviz/bi/grid/Grid.js";
import { handleHideShowSubmenu } from "../../workspace/generic-util.js";
import { Workspace } from "../../workspace/Workspace.js";
import { isNonDuckDBDestination, generateInitialQuery } from "../../../services/DestinationUtil.js";
import { PipelineService } from "../../../services/PipelineService.js";

export class SqlEditor extends ViewComponent {

	isPublic = true;

	/** @Prop */ query;

	/** @Prop */ editor;

	/** @Prop */ uniqueId = '_'+UUIDUtil.newId();

	/** @type { Workspace }  */ $parent;

	/** @Prop @type { Grid }  */ queryOutput;

	/** @Prop */ database;

	/** @type { State<String> } */ databasename = null;

	/** @Prop */ dbpath;

	/** @type { ListState<Array> } */ selectedTableFields = [];

	/** @type { ListState<Array> } */ tablesList = [];

	/** @Prop */ selectedTable;
	
	/** @Prop */ connectionName;
	
	/** @Prop */ destType;
	
	/** @Prop */ pplineName;
	
	/** @Prop */ dbEngine;  // Database engine type for SQL dialect handling
	
	/** 
	 * @param {Object} param0 
	 * @param {String} param0.database  */
	async stOnRender({ query, database, databaseParam, queryTable, connectionName, destType, pplineName }){

		await this.$parent.controller.loadMonacoEditorDependencies();
		
		let dbPath = null, databasename = '';
		if(database){
			dbPath = database.split('/');
			databasename = dbPath.pop()+'.'+queryTable;
		}else if(databaseParam)
			databasename = databaseParam;
		
		this.query = query;
		this.connectionName = connectionName;
		this.destType = destType || 'duckdb';
		this.pplineName = pplineName;

		this.tablesList = await this.$parent.service.getParsedTables(this.$parent.socketData.sid);
		this.dbpath = this.$parent.service.dbPath.slice(0,-1);
		
		// Initialize databasename with the correct value BEFORE accessing fieldsByTableMap
		this.databasename = this.destType === 'duckdb' ? databasename.split('.').slice(1).join('.') : databasename;
		
		if(this.$parent.service.fieldsByTableMap[databasename.trim()])
			this.selectedTableFields = this.$parent.service.fieldsByTableMap[databasename.trim()];
		if (Array.isArray(this.selectedTableFields))
			this.selectedTableFields = [{ name: 'Fields in the table', type: '' }, ...this.selectedTableFields];

		// Handle database path based on destination type
		if (isNonDuckDBDestination(this.destType)) {
			// For SQL, BigQuery, and Databricks destinations, we don't need a file path
			this.database = null;
		} else {
			// For DuckDB, parse the filename from databasename format: "ppline.dbfile.duckdb.schema.table"
			const parts = databasename.split('.');
			const duckdbIndex = parts.indexOf('duckdb');
			
			if (duckdbIndex > 0) {
				// Get the database file name (part before 'duckdb')
				const parseDbFilename = `${parts[duckdbIndex - 1]}.duckdb`;
				this.database = `${this.dbpath}/${parseDbFilename}`;
			} else {
				// Fallback: try old format
				const parseDbFilename = `${databasename.split('.duckdb.')[0]}.duckdb`;
				this.database = `${this.dbpath}/${parseDbFilename}`;
			}
		}
	}

	async stAfterInit() {

		this.editor = monaco.editor.create(document.getElementById(this.uniqueId), {
			value: this.query, language: 'sql',
			theme: 'vs-light', automaticLayout: true,
			minimap: { enabled: false }, scrollBeyondLastLine: false,
			fontSize: 14
		});
		
		this.handleHideShowFieldMenu();
		this.databasename.onChange((databaseName) => this.setupEditorQuery(databaseName));
		if(this.databasename !== null) this.setupEditorQuery(this.databasename.value.trim());
	}

	setupEditorQuery(databaseName){
		// Handle table name extraction based on destination type
		PipelineService.sqlEditorDestSecretName = null, PipelineService.sqlEditorDestType = null;
		PipelineService.pipelineReferencedSecrets = null, PipelineService.pipelineDestinationConfig = null;
		let selectedTable, sourceAndDestType;

		const parts = databaseName.split('.');
		const duckdbIndex = parts.indexOf('duckdb');
		const key = parts[0] == '' ? this.pplineName : parts[0];
		sourceAndDestType = PipelineService.pipelineSourcesAndSestinationsMap[key];

		if (!(duckdbIndex > 0) && sourceAndDestType?.destType !== 'DUCKDB_DEST') {
			// For SQL, BigQuery, and Databricks: databaseName is in format "ppline.schema.table"
			// We need to strip the pipeline name prefix to get "schema.table"
			if (parts.length >= 3) {
				// Remove first part (pipeline name) and keep "schema.table"
				selectedTable = parts.slice(1).join('.');
			} else {
				// Fallback: use as-is if format is unexpected
				selectedTable = databaseName;
			}
		} else {
			// For DuckDB: databaseName is in format "ppline.dbfile.duckdb.schema.table"
			// Example: "duckdb_test.duckdb_test.duckdb.main.people"
			// We need to extract "dbfile.schema.table" format: "duckdb_test.main.people"
			
			PipelineService.sqlEditorDestSecretName = null, PipelineService.sqlEditorDestType = 'duckdb';

			if (duckdbIndex > 0 && parts.length > duckdbIndex + 1) {
				// Get the database file name (part before 'duckdb')
				const dbFileName = parts[duckdbIndex - 1];
				// Get schema and table (parts after 'duckdb')
				const schemaAndTable = parts.slice(duckdbIndex + 1).join('.');
				// Construct: dbfile.schema.table
				selectedTable = `${dbFileName}.${schemaAndTable}`;
			} else {
				// Fallback: try old format "dbfile.duckdb.schema.table"
				const splitResult = databaseName.split('.duckdb.');
				if (splitResult.length > 1) 
					selectedTable = `${splitResult[0].split('.').pop()}.${splitResult[1]}`;
				else 
					selectedTable = databaseName;
			}
		}
		
		this.selectedTable = selectedTable;
		const fieldsListMap = this.$parent.service.fieldsByTableMap;
		const selectedTableFields = fieldsListMap[sourceAndDestType?.destType === 'DUCKDB_DEST' ? selectedTable : databaseName];
		const fieldsArray = selectedTableFields ? selectedTableFields.map(({ name }) => name) : [];
		
		if (Array.isArray(selectedTableFields))
			this.selectedTableFields = [{ name: 'Fields in the table', type: '' }, ...selectedTableFields];

		// Generate query using database-specific syntax
		const query = generateInitialQuery(this.selectedTable, fieldsArray, sourceAndDestType, 100);
		this.setCode(AIResponseLinterUtil.formatSQL(query));

		if (duckdbIndex > 0) {
			// Get the database file name (part before 'duckdb')
			const parseDbFilename = `${parts[duckdbIndex - 1]}.duckdb`;
			this.database = `${this.dbpath}/${parseDbFilename}`;
		} else {
			// Fallback: try old format
			const parseDbFilename = `${databaseName.split('.duckdb.')[0]}.duckdb`;
			this.database = `${this.dbpath}/${parseDbFilename}`;
		}
	}

	handleHideShowFieldMenu = () => handleHideShowSubmenu('.data-base-fields-submenu', '.submenu');
	
	setCode = (code) => this.editor.setValue(code);

	async runSQLQuery(){

		const newQuery = this.editor.getValue();
		this.queryOutput.stAfterInit(null, true);

		const { result, fields, error, db_engine } = await PipelineService.runSQLQuery(
			newQuery, 
			this.database, 
			PipelineService.sqlEditorDestSecretName || this.connectionName, 
			PipelineService.sqlEditorDestType || this.destType
		);
		
		// Store db_engine for future queries
		if (db_engine) this.dbEngine = db_engine;
		
		const parsedFields = (fields || '').replaceAll('\n', '')?.split(',')?.map(field => field.trim());		
		this.queryOutput.setGridData(parsedFields, result).stAfterInit(error);
	}

}