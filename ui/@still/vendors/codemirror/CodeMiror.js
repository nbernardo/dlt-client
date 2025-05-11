import { ViewComponent } from "../../component/super/ViewComponent.js";
import { UUIDUtil } from "../../util/UUIDUtil.js";

export class CodeMiror extends ViewComponent {

    /** @Prop */
    codeEditor;

    isPublic = true;

    /** @Prop */
    editorHeight;

    /** @Prop */
    startHeight;
    
    /** @Prop */
    containerId = `_${UUIDUtil.newId()}`;
    
    template = `
        <div id="@containerId">
            <textarea id="@dynCmpGeneratedId"></textarea>
        </div>
        <style>
            .CodeMirror-line{ padding-left: 35px !important; }
        </style>
    `;

    stAfterInit(){
        
        this.codeEditor = CodeMirror.fromTextArea(
            document.getElementById(this.dynCmpGeneratedId), {
			lineNumbers: true,
			mode: 'python',
			theme: 'monokai',
			language: 'python'
		});
        
        document.getElementById(this.containerId).style.height = this.startHeight + 'px';
        this.codeEditor.setSize(null, Number(this.editorHeight))
        this.emit('load');
    }

    async load() {}

    setHeight(value){
        this.editorHeight = value;
        this.codeEditor.setSize(null, value);
        document.getElementById(this.containerId).style.height = value + 'px';
    }

    getHeight(){
        return this.editorHeight;
    }



    runCode(){}

}