import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { Assets } from "../../../../@still/util/componentUtil.js";
import { UUIDUtil } from "../../../../@still/util/UUIDUtil.js";

export class SqlEditor extends ViewComponent {

	isPublic = true;

	/** @Prop */ query;

	/** @Prop */ editor;

	async stBeforeInit() {
		
		if (window.monaco) return;
		
		await Assets.import({ path: 'https://cdn.jsdelivr.net/npm/showdown/dist/showdown.min.js' });
		await Assets.import({ path: 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.46.0/min/vs/loader.min.js' });

		require.config({ paths: { 'vs': 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.46.0/min/vs' } });
		require(['vs/editor/editor.main'], () => window.monaco);

	}

	stOnRender({ query }){
		this.query = query;
	}

	stAfterInit() {

		this.editor = monaco.editor.create(document.querySelector('#extend-view-code-editor'), {
			value: this.query,
			language: 'sql',
			theme: 'vs-light',
			automaticLayout: true,
			minimap: { enabled: false },
			scrollBeyondLastLine: false,
			fontSize: 14
		})
		
	}

}