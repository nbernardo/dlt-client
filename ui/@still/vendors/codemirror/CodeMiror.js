import { ViewComponent } from "../../component/super/ViewComponent.js";

export class CodeMiror extends ViewComponent {

    /** @Prop */
    codeEditorInput;
    /** @Prop */
    codeEditor;

    isPublic = true;
    
    template = `
        <textarea id="@dynCmpGeneratedId"></textarea>
    `;

    stAfterInit(){
        this.codeEditorInput = this.dynCmpGeneratedId;

        this.codeEditor = CodeMirror.fromTextArea(
            document.getElementById(this.codeEditorInput), {
			lineNumbers: true,
			mode: 'python',
			theme: 'monokai',
			language: 'python'
		});
        this.emit('load');
    }

    async load() {}

    runCode(){}

}