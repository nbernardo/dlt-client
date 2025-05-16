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
    
    /** @Prop */
    availableLangs;

    template = `
        <div id="@containerId">
            <textarea id="@dynCmpGeneratedId"></textarea>
        </div>
        <style>
            .CodeMirror-line{ padding-left: 35px !important; }
        </style>
    `;

    stAfterInit(){
        
        this.setAvailableLanguages();

        // This is to make sure that code editor instantiates 
        // only when the Library was loaded and is ready to be used
        const editorTimer = setInterval(() => {
            if(window.CodeMirror){
                clearInterval(editorTimer);
                this.codeEditor = CodeMirror.fromTextArea(
                    document.getElementById(this.dynCmpGeneratedId), {
                    lineNumbers: true,
                    mode: 'python',
                    theme: 'monokai',
                });
                        
                document.getElementById(this.containerId).style.height = this.startHeight + 'px';
                this.codeEditor.setSize(null, Number(this.editorHeight))
                this.emit('load');
            }
        },500);
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

    changeLanguage(lang){
        this.codeEditor.setOption('mode',this.availableLangs[lang]);
    }

    setAvailableLanguages(){
        this.availableLangs = {
            'python-lang': 'python',
            //Bellow is only ANSI SQL
            'sql-lang': 'text/x-sql' 
        };
    }

    setCode(value){
        this.codeEditor.setValue(value);
    }

}