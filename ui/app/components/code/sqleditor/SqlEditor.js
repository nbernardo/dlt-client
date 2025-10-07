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

	async stBeforeInit() {
		
		if (window.monaco) return;
		
		await Assets.import({ path: 'https://cdn.jsdelivr.net/npm/showdown/dist/showdown.min.js' });
		await Assets.import({ path: 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.46.0/min/vs/loader.min.js' });

		require.config({ paths: { 'vs': 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.46.0/min/vs' } });
		require(['vs/editor/editor.main'], () => window.monaco);

	}

	/** 
	 * @param {Object} param0 
	 * @param {String} param0.database  */
	async stOnRender({ query, database, databaseParam, queryTable }){

		let dbPath = null, databasename = '';
		if(database){
			dbPath = database.split('/')
			databasename = dbPath.pop()+'.'+queryTable;
		}else if(databaseParam)
			databasename = databaseParam;
		
		this.query = query;
		const user = this.$parent.userEmail, socketId = this.$parent.socketData.sid;
		this.tablesList = await this.$parent.service.getParsedTables(user, socketId);
		this.dbpath = this.$parent.service.dbPath.slice(0,-1);
		
		if(this.$parent.service.fieldsByTableMap[databasename.trim()])
			this.selectedTableFields = this.$parent.service.fieldsByTableMap[databasename.trim()];

		const parseDbFilename = `${databasename.split('.duckdb.')[0]}.duckdb`;
		this.database = `${this.dbpath}/${parseDbFilename}`;
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
		this.selectedTable = databaseName.split('.duckdb.')[1];
		this.selectedTableFields = this.$parent.service.fieldsByTableMap[databaseName];
		const fieldsString = this.selectedTableFields.value.map(({ name }) => name).join(',');
		const query = `SELECT ${fieldsString} FROM ${this.selectedTable} LIMIT 100`
		this.setCode(AIResponseLinterUtil.formatSQL(query));

		const parseDbFilename = `${databaseName.split('.duckdb.')[0]}.duckdb`;
		this.database = `${this.dbpath}/${parseDbFilename}`;
	}

	handleHideShowFieldMenu = () => handleHideShowSubmenu('.data-base-fields-submenu', '.submenu');
	
	setCode = (code) => this.editor.setValue(code);

	async runSQLQuery(){
		const newQuery = this.editor.getValue();
		const { result, fields } = await this.$parent.service.runSQLQuery(newQuery, this.database);
		const parsedFields = fields.replaceAll('\n', '').split(',').map(field => field.trim());
		this.queryOutput.setGridData(parsedFields, result).stAfterInit();
	}

}