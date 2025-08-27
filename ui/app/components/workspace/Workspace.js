import { io as SocketIO } from "https://cdn.socket.io/4.8.1/socket.io.esm.min.js";
import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { Components } from "../../../@still/setup/components.js";
import { AppTemplate } from "../../../config/app-template.js";
import { NodeTypeEnum, PPLineStatEnum, WorkSpaceController } from "../../controller/WorkSpaceController.js";
import { PipelineService } from "../../services/PipelineService.js";
import { ObjectDataTypes, WorkspaceService } from "../../services/WorkspaceService.js";
import { CodeMiror } from "../../../@still/vendors/codemirror/CodeMiror.js";
import { Terminal } from "./terminal/Terminal.js";
import { SqlDBComponent } from "../node-types/SqlDBComponent.js";
import { EditorLanguageType } from "../../types/editor.js";
import { StillTreeView } from "../../../@still/vendors/treeview/StillTreeView.js";
import { connectIcon, copyClipboardIcin, dbIcon, pipelineIcon, tableIcon, tableToTerminaIcon, viewpplineIcon } from "./icons/database.js";
import { StillDivider } from "../../../@still/component/type/ComponentType.js";
import { UserService } from "../../services/UserService.js";
import { PopupWindow } from "../popup-window/PopupWindow.js";
import { LeftTabs } from "../navigation/left/LeftTabs.js";
import { NoteBook } from "../code/NoteBook.js";

export class Workspace extends ViewComponent {

	isPublic = false;

	/** @Prop */
	editor;

	/**
	 * @Inject
	 * @Path services/
	 * @type { PipelineService } */
	pplService;

	/**
	 * @Inject
	 * @Path services/
	 * @type { WorkspaceService } */
	service;

	/**
	 * @Controller
	 * @Path controller/
	 * @type { WorkSpaceController } */
	controller;

	/** @type { Array<ObjectDataTypes> } */
	objectTypes = [];

	/** @Prop */
	socketData = { sid: null };

	activeGrid = "Enter pipeline name";

	/** 
	 * @Proxy 
	 * @type { CodeMiror }*/
	cmProxy;

	/** 
	 * @Proxy 
	 * @type { Terminal }*/
	terminalProxy;

	/** 
	 * @Inject 
	 * @Path services/ 
	 * @type { UserService }*/
	userService;

	/** 
	 * @Prop 
	 * @type { EditorLanguageType }
	 * */
	editorActiveLang = EditorLanguageType.PYTHON;

	/** @Prop */
	selectedLangClass = 'editor-lang-mode-selected';
	/** @Prop */
	noSelectedLangClass = 'editor-lang-mode';
	/** @Prop @type { HTMLElement } */
	drawFlowContainer;
	/** @Prop @type { Set } */
	connectedDbs = new Set();
	
	/**
	 * This is also the width added
	 * in the CSS concerning to the left side 
	 * @Prop 
	 * */
	startLeftWidth = 300;
	/** @Prop */
	pxToVw = 0.100;

	/** @Prop */
	showDbTreeViewBullets = false;

	/** @Proxy @type { StillDivider } */
	codeEditorSplitter;

	/** @Proxy @type { PopupWindow } */
	popupWindowProxy;

	/** @Proxy @type { NoteBook } */
	noteBookProxy;

	/** @Proxy @type { LeftTabs } */
	leftMenuProxy;

	/** @Prop */
	isEditorOpened = false;

	/** @Prop */
	showLoading = true;

	/** @Prop */
	anyPropTest = 0;

	loggedUser = null;

	/** @Prop */ userEmail = null;

	/** @Prop */ showDrawFlow = true;

	selectedLeftTab = 'content-diagram';

	stOnRender() {
		this.service.on('load', () => {
			this.objectTypes = this.service.objectTypes;
			this.service.table.onChange(newValue => {
				console.log(`Workspace was update about changed and new value is: `, newValue);
			});
		});

		this.userService.on('load', async () => {
			const user = (await this.userService.getLoggedUser());
			this.loggedUser = user.name;
			this.userEmail = user.email;
		});
	}

	async stAfterInit() {
		this.drawFlowContainer = document.getElementById('drawflow');
		this.pplService.on('load', () => {
			console.log(`Service loaded with updated: `, this.pplService);
			/* this.service.createPipeline().then(res => {
				console.log(`Pipeline created successfully: `, res);
			}) */
		});
		this.buildWorkspaceView();
		setTimeout(() =>  this.showLoading = false, 1500);
		this.onLeftTabChange();
		//this.cmProxy.codeEditor.setSize(null, 100);
	}

	onLeftTabChange(){
		this.selectedLeftTab.onChange(activeTab => {
			document.getElementsByClassName(activeTab)[0].style.width = '250px';
			document.getElementsByClassName('separator')[0].style.marginLeft = '150px';
		});
	}

	buildWorkspaceView() {

		this.controller.on('load', () => {

			this.controller.registerEvents();
			this.controller.socketChannelSetup(SocketIO, this.socketData);

			var id = document.getElementById("drawflow");
			this.editor = new Drawflow(id);
			this.controller.editor = this.editor;
			this.editor.reroute = true;
			//const dataToImport = { "drawflow": { "Home": { "data": { "1": { "id": 1, "name": "welcome", "data": {}, "class": "welcome", "html": "\n    <div>\n      <div class=\"title-box\">👏 Welcome!!</div>\n      <div class=\"box\">\n        <p>Simple flow library <b>demo</b>\n        <a href=\"https://github.com/jerosoler/Drawflow\" target=\"_blank\">Drawflow</a> by <b>Jero Soler</b></p><br>\n\n        <p>Multiple input / outputs<br>\n           Data sync nodes<br>\n           Import / export<br>\n           Modules support<br>\n           Simple use<br>\n           Type: Fixed or Edit<br>\n           Events: view console<br>\n           Pure Javascript<br>\n        </p>\n        <br>\n        <p><b><u>Shortkeys:</u></b></p>\n        <p>🎹 <b>Delete</b> for remove selected<br>\n        💠 Mouse Left Click == Move<br>\n        ❌ Mouse Right == Delete Option<br>\n        🔍 Ctrl + Wheel == Zoom<br>\n        📱 Mobile support<br>\n        ...</p>\n      </div>\n    </div>\n    ", "typenode": false, "inputs": {}, "outputs": {}, "pos_x": 50, "pos_y": 50 }, "2": { "id": 2, "name": "slack", "data": {}, "class": "slack", "html": "\n          <div>\n            <div class=\"title-box\"><i class=\"fab fa-slack\"></i> Slack chat message</div>\n          </div>\n          ", "typenode": false, "inputs": { "input_1": { "connections": [{ "node": "7", "input": "output_1" }] } }, "outputs": {}, "pos_x": 1028, "pos_y": 87 }, "3": { "id": 3, "name": "telegram", "data": { "channel": "channel_2" }, "class": "telegram", "html": "\n          <div>\n            <div class=\"title-box\"><i class=\"fab fa-telegram-plane\"></i> Telegram bot</div>\n            <div class=\"box\">\n              <p>Send to telegram</p>\n              <p>select channel</p>\n              <select df-channel>\n                <option value=\"channel_1\">Channel 1</option>\n                <option value=\"channel_2\">Channel 2</option>\n                <option value=\"channel_3\">Channel 3</option>\n                <option value=\"channel_4\">Channel 4</option>\n              </select>\n            </div>\n          </div>\n          ", "typenode": false, "inputs": { "input_1": { "connections": [{ "node": "7", "input": "output_1" }] } }, "outputs": {}, "pos_x": 1032, "pos_y": 184 }, "4": { "id": 4, "name": "email", "data": {}, "class": "email", "html": "\n            <div>\n              <div class=\"title-box\"><i class=\"fas fa-at\"></i> Send Email </div>\n            </div>\n            ", "typenode": false, "inputs": { "input_1": { "connections": [{ "node": "5", "input": "output_1" }] } }, "outputs": {}, "pos_x": 1033, "pos_y": 439 }, "5": { "id": 5, "name": "template", "data": { "template": "Write your template" }, "class": "template", "html": "\n            <div>\n              <div class=\"title-box\"><i class=\"fas fa-code\"></i> Template</div>\n              <div class=\"box\">\n                Ger Vars\n                <textarea df-template></textarea>\n                Output template with vars\n              </div>\n            </div>\n            ", "typenode": false, "inputs": { "input_1": { "connections": [{ "node": "6", "input": "output_1" }] } }, "outputs": { "output_1": { "connections": [{ "node": "4", "output": "input_1" }, { "node": "11", "output": "input_1" }] } }, "pos_x": 607, "pos_y": 304 }, "6": { "id": 6, "name": "github", "data": { "name": "https://github.com/jerosoler/Drawflow" }, "class": "github", "html": "\n          <div>\n            <div class=\"title-box\"><i class=\"fab fa-github \"></i> Github Stars</div>\n            <div class=\"box\">\n              <p>Enter repository url</p>\n            <input type=\"text\" df-name>\n            </div>\n          </div>\n          ", "typenode": false, "inputs": {}, "outputs": { "output_1": { "connections": [{ "node": "5", "output": "input_1" }] } }, "pos_x": 341, "pos_y": 191 }, "7": { "id": 7, "name": "facebook", "data": {}, "class": "facebook", "html": "\n        <div>\n          <div class=\"title-box\"><i class=\"fab fa-facebook\"></i> Facebook Message</div>\n        </div>\n        ", "typenode": false, "inputs": {}, "outputs": { "output_1": { "connections": [{ "node": "2", "output": "input_1" }, { "node": "3", "output": "input_1" }, { "node": "11", "output": "input_1" }] } }, "pos_x": 347, "pos_y": 87 }, "11": { "id": 11, "name": "log", "data": {}, "class": "log", "html": "\n            <div>\n              <div class=\"title-box\"><i class=\"fas fa-file-signature\"></i> Save log file </div>\n            </div>\n            ", "typenode": false, "inputs": { "input_1": { "connections": [{ "node": "5", "input": "output_1" }, { "node": "7", "input": "output_1" }] } }, "outputs": {}, "pos_x": 1031, "pos_y": 363 } } }, "Other": { "data": { "8": { "id": 8, "name": "personalized", "data": {}, "class": "personalized", "html": "\n            <div>\n              Personalized\n            </div>\n            ", "typenode": false, "inputs": { "input_1": { "connections": [{ "node": "12", "input": "output_1" }, { "node": "12", "input": "output_2" }, { "node": "12", "input": "output_3" }, { "node": "12", "input": "output_4" }] } }, "outputs": { "output_1": { "connections": [{ "node": "9", "output": "input_1" }] } }, "pos_x": 764, "pos_y": 227 }, "9": { "id": 9, "name": "dbclick", "data": { "name": "Hello World!!" }, "class": "dbclick", "html": "\n            <div>\n            <div class=\"title-box\"><i class=\"fas fa-mouse\"></i> Db Click</div>\n              <div class=\"box dbclickbox\" ondblclick=\"showpopup(event)\">\n                Db Click here\n                <div class=\"modal\" style=\"display:none\">\n                  <div class=\"modal-content\">\n                    <span class=\"close\" onclick=\"closemodal(event)\">&times;</span>\n                    Change your variable {name} !\n                    <input type=\"text\" df-name>\n                  </div>\n\n                </div>\n              </div>\n            </div>\n            ", "typenode": false, "inputs": { "input_1": { "connections": [{ "node": "8", "input": "output_1" }] } }, "outputs": { "output_1": { "connections": [{ "node": "12", "output": "input_2" }] } }, "pos_x": 209, "pos_y": 38 }, "12": { "id": 12, "name": "multiple", "data": {}, "class": "multiple", "html": "\n            <div>\n              <div class=\"box\">\n                Multiple!\n              </div>\n            </div>\n            ", "typenode": false, "inputs": { "input_1": { "connections": [] }, "input_2": { "connections": [{ "node": "9", "input": "output_1" }] }, "input_3": { "connections": [] } }, "outputs": { "output_1": { "connections": [{ "node": "8", "output": "input_1" }] }, "output_2": { "connections": [{ "node": "8", "output": "input_1" }] }, "output_3": { "connections": [{ "node": "8", "output": "input_1" }] }, "output_4": { "connections": [{ "node": "8", "output": "input_1" }] } }, "pos_x": 179, "pos_y": 272 } } } } }
			this.editor.start();
			//editor.import(dataToImport);
			this.controller.handleListeners(this.editor);
		});
	}

	async savePipeline() {
		if(this.activeGrid === 'Enter pipeline name') {
			return AppTemplate.toast.error('Please enter a valid pipeline name');
		}
		this.controller.pplineStatus = PPLineStatEnum.Start;
		const formReferences = [...this.controller.formReferences.values()];
		const validationResults = formReferences.map((r) => {
			const component = Components.ref(r);
			if(component.getName() === SqlDBComponent.name) component.getTables();
			const form = component.formRef;
			return form?.validate();
		});

		const anyInvalidForm = validationResults.indexOf(false) >= 0;
		const isValidSubmission = this.handleSubmissionError(anyInvalidForm);
		if (!isValidSubmission) return;

		let data = this.editor.export();
		const startNode = this.controller.edgeTypeAdded[NodeTypeEnum.START];
		const activeGrid = this.activeGrid.value.toLowerCase().replace(/\s/g, '_');
		data = { ...data, user: this.userEmail, startNode, activeGrid, pplineLbl: this.activeGrid.value, socketSid: this.socketData.sid }
		console.log(data);
		const result = await this.pplService.createPipeline(data);
	}

	resetWorkspace(){
		this.editor.clearModuleSelected();
		this.controller.resetEdges();
		document.getElementById('pplineNamePlaceHolder').innerHTML = 'Enter pipeline name';
	}

	onPplineNameKeyPress(e) {
		if (e.key === 'Enter') {
			e.preventDefault();
			this.activeGrid = e.target.innerText;
			e.target.blur();
			e.target.style.fontWeight = 'bold';
		}
	}

	handleSubmissionError(anyInvalidForm) {

		this.controller.clearValidationError();
		if (anyInvalidForm)
			this.controller.addWorkspaceError(`Please fill all the steps accordingly`);

		const initNode = this.controller.checkStartNode();
		if (!initNode)
			this.controller.addWorkspaceError(`Please add start node`);

		let unlinkNodeErrorCounter = 0;
		Object.entries(this.controller.edgeTypeAdded).forEach(([id, relations]) => {

			if (this.controller.edgeTypeAdded[NodeTypeEnum.START] != id
				|| this.controller.edgeTypeAdded[NodeTypeEnum.END] != id
			) {
				if (relations.size == 0) {
					unlinkNodeErrorCounter++;
					const { data } = WorkSpaceController.getNode(id);
					if (data?.componentId) {
						const ipts = this.controller.getInputsByNodeId(data?.componentId);
						[...ipts].forEach(elm => elm.className = `${elm.className} blink`);
					}
				}
			}
		})

		if (unlinkNodeErrorCounter > 0)
			this.controller.addWorkspaceError(`Please link all nodes accordingly`);

		const outputError = document.createElement('ul');
		const errors = this.controller.validationErrors;
		for (const error of errors) outputError.appendChild(error);

		if (errors.length) AppTemplate.toast.error(outputError.innerHTML);

		if (!!anyInvalidForm || !initNode || unlinkNodeErrorCounter > 0) return false;
		return true;
	}

	onSplitterMove(params){
		// 65 is the minimun height set on the template (Workspace.html)
		if(params.bottomHeight <= 65) this.isEditorOpened = false;
		else this.isEditorOpened = true;
		const editorHeight = ((params.bottomHeight) * 50) / 100;
		this.cmProxy.setHeight(editorHeight);
		this.terminalProxy.resizeHeight(editorHeight);
	}

	async runCode(){
		const code = this.cmProxy.codeEditor.getValue();
		const database = [...this.connectedDbs][0]
		const payload = { 
			code, lang: this.editorActiveLang, 
			session: this.socketData.sid, database
		};

		let result = await this.service.runCode(payload, this.userEmail);
		result = await result.json();
		this.terminalProxy.writeTerminal(result.output);
	}

	selecteLang(lang){
		const langs = document.querySelectorAll('.editor-lang');
		this.cmProxy.changeLanguage(lang);
		this.editorActiveLang = lang;
		langs.forEach(elm => {
			if(elm.classList.contains(lang)){
				elm.classList.remove(this.noSelectedLangClass);
				elm.classList.add(this.selectedLangClass);
			}else{
				elm.classList.add(this.noSelectedLangClass);
				elm.classList.remove(this.selectedLangClass);
			}
		})
	}

	async connectToDatabase(event, dbName){

		event.preventDefault();
		const element = event.target;
		const database = dbName;
		const session = this.socketData.sid;
		const payload = { session, database };
		let result;
		
		if(this.connectedDbs.has(dbName)){
			result = await (
				await this.service.handleDuckdbConnect(payload, WorkspaceService.DISCONECT_DB)
			).json();

			if(result.status){
				this.connectedDbs.delete(dbName);
				element.style.color = 'grey';
			}

		} else{
			result = await (await this.service.handleDuckdbConnect(payload)).json();
			if(result.status){
				this.connectedDbs.add(dbName);
				element.style.color = 'green';
			}
		}

		if(!result.status) AppTemplate.toast.error(result.message);

	}

	genInitialDBQuery(table, dbfile){
		this.selecteLang('sql-lang');
		this.cmProxy.setCode(`use ${dbfile};\n\nSELECT * FROM ${table};`);
		this.codeEditorSplitter.setMaxHeight();
		this.isEditorOpened = true;
	}

	showHideEditor(){
		if(this.isEditorOpened){
			// 88 Is because we're taking into account the splitter height itself 
			this.codeEditorSplitter.setHeight(88);
			this.isEditorOpened = false;
		}else{
			this.codeEditorSplitter.setMaxHeight();
			this.isEditorOpened = true;
		}
	}

	viewPipelineDiagram(event, data){
		event.preventDefault();
		console.log(data);
	}

	async logout(){
		await this.userService.logOut();
	}

	verticalResize({ leftWidth }){
		const selectedTab = this.selectedLeftTab.value;
		document.getElementsByClassName(selectedTab)[0].style.width = (leftWidth+100)+'px';
	}

	async viewScriptOnEditor(){
		const fileName = this.leftMenuProxy.scriptListProxy.selectedFile;
		const code = await this.service.readScriptFile(this.userEmail, fileName);
		this.noteBookProxy.openFile = {fileName, code};
		this.noteBookProxy.showNotebook = true;
		this.showDrawFlow = false;
	}

	viewFileOnEditor(){
		this.leftMenuProxy.scriptListProxy.selectedFile;
		console.log(`WHEN CALLING FROM FILE: `,this.leftMenuProxy.fileListProxy.selectedFile);
	}

}