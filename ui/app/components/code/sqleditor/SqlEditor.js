import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { ListState, State } from "../../../../@still/component/type/ComponentType.js";
import { Assets } from "../../../../@still/util/componentUtil.js";
import { UUIDUtil } from "../../../../@still/util/UUIDUtil.js";
import { AIResponseLinterUtil } from "../../agent/AIResponseLinterUtil.js";
import { Grid } from "../../grid/Grid.js";
import { handleHideShowSubmenu } from "../../workspace/generic-util.js";
import { Workspace } from "../../workspace/Workspace.js";

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
	
	/** 
	 * @param {Object} param0 
	 * @param {String} param0.database  */
	async stOnRender({ query, database, databaseParam, queryTable, connectionName, destType, pplineName }){

		await this.$parent.controller.loadMonacoEditorDependencies();
		
		let dbPath = null, databasename = '';
		if(database){
			dbPath = database.split('/')
			databasename = dbPath.pop()+'.'+queryTable;
		}else if(databaseParam)
			databasename = databaseParam;
		
		this.query = query;
		this.connectionName = connectionName;
		this.destType = destType || 'duckdb';
		this.pplineName = pplineName;

		this.tablesList = await this.$parent.service.getParsedTables(this.$parent.socketData.sid);
		this.dbpath = this.$parent.service.dbPath.slice(0,-1);
		
		if(this.$parent.service.fieldsByTableMap[databasename.trim()])
			this.selectedTableFields = this.$parent.service.fieldsByTableMap[databasename.trim()];
		if (Array.isArray(this.selectedTableFields))
			this.selectedTableFields = [{ name: 'Fields in the table', type: '' }, ...this.selectedTableFields];

		// Handle database path based on destination type
		if (this.destType === 'sql') {
			// For SQL destinations, we don't need a file path
			this.database = null;
		} else {
			// For DuckDB, parse the filename
			const parseDbFilename = `${databasename.split('.duckdb.')[0]}.duckdb`;
			this.database = `${this.dbpath}/${parseDbFilename}`;
		}
		this.databasename = databasename;
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
		let selectedTable;
		if (this.destType === 'sql') {
			// For SQL destinations: databaseName is already in format "schema.table"
			selectedTable = databaseName;
		} else {
			// For DuckDB: extract table from "dbfile.duckdb.schema.table" format
			selectedTable = databaseName.split('.duckdb.')[1];
		}
		
		this.selectedTable = selectedTable;
		const selectedTableFields = this.$parent.service.fieldsByTableMap[databaseName];
		const fieldsString = selectedTableFields.map(({ name }) => name).join(',');
		
		if (Array.isArray(selectedTableFields))
			this.selectedTableFields = [{ name: 'Fields in the table', type: '' }, ...selectedTableFields];

		const query = `SELECT ${fieldsString} FROM ${this.selectedTable} LIMIT 100`
		this.setCode(AIResponseLinterUtil.formatSQL(query));

		// Update database path for DuckDB only
		if (this.destType !== 'sql') {
			const parseDbFilename = `${databaseName.split('.duckdb.')[0]}.duckdb`;
			this.database = `${this.dbpath}/${parseDbFilename}`;
		}
	}

	handleHideShowFieldMenu = () => handleHideShowSubmenu('.data-base-fields-submenu', '.submenu');
	
	setCode = (code) => this.editor.setValue(code);

	async runSQLQuery(){
		const newQuery = this.editor.getValue();
		const { result, fields, error } = await this.$parent.service.runSQLQuery(
			newQuery, 
			this.database, 
			this.connectionName, 
			this.destType
		);
		const parsedFields = (fields || '').replaceAll('\n', '')?.split(',')?.map(field => field.trim());
		this.queryOutput.setGridData(parsedFields, result).stAfterInit(error);
	}

}