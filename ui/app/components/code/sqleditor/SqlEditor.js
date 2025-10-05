import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { Assets } from "../../../../@still/util/componentUtil.js";
import { UUIDUtil } from "../../../../@still/util/UUIDUtil.js";
import { Grid } from "../../grid/Grid.js";
import { Workspace } from "../../workspace/Workspace.js";

export class SqlEditor extends ViewComponent {

	isPublic = true;

	/** @Prop */ query;

	/** @Prop */ editor;

	/** @Prop */ uniqueId = '_'+UUIDUtil.newId();

	/** @type { Workspace }  */ $parent;

	/** @Prop @type { Grid }  */ queryOutput;

	/** @Prop */ database;

	tablesList;

	async stBeforeInit() {
		
		if (window.monaco) return;
		
		await Assets.import({ path: 'https://cdn.jsdelivr.net/npm/showdown/dist/showdown.min.js' });
		await Assets.import({ path: 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.46.0/min/vs/loader.min.js' });

		require.config({ paths: { 'vs': 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.46.0/min/vs' } });
		require(['vs/editor/editor.main'], () => window.monaco);


	}

	async stOnRender({ query }){
		this.query = query;
		const user = this.$parent.userEmail, socketId = this.$parent.socketData.sid;
		this.tablesList = await this.$parent.service.getParsedTables(user, socketId);
	}

	async stAfterInit() {

		this.editor = monaco.editor.create(document.getElementById(this.uniqueId), {
			value: this.query,
			language: 'sql',
			theme: 'vs-light',
			automaticLayout: true,
			minimap: { enabled: false },
			scrollBeyondLastLine: false,
			fontSize: 14
		});
	}

	async runSQLQuery(){
		const newQuery = this.editor.getValue();
		const { result, fields } = await this.$parent.service.runSQLQuery(newQuery, this.database);
		const parsedFields = fields.replaceAll('\n', '').split(',').map(field => field.trim());
		this.queryOutput.setGridData(parsedFields, result).loadGrid();
	}

}