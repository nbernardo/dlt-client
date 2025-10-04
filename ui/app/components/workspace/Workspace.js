import { io as SocketIO } from "https://cdn.socket.io/4.8.1/socket.io.esm.min.js";
import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { Components } from "../../../@still/setup/components.js";
import { AppTemplate } from "../../../config/app-template.js";
import { NodeTypeEnum, PPLineStatEnum, WorkSpaceController } from "../../controller/WorkSpaceController.js";
import { PipelineService } from "../../services/PipelineService.js";
import { ObjectDataTypes, WorkspaceService } from "../../services/WorkspaceService.js";
import { SqlDBComponent } from "../node-types/SqlDBComponent.js";
import { EditorLanguageType } from "../../types/editor.js";
import { StillDivider } from "../../../@still/component/type/ComponentType.js";
import { UserService } from "../../services/UserService.js";
import { PopupWindow } from "../popup-window/PopupWindow.js";
import { LeftTabs } from "../navigation/left/LeftTabs.js";
import { NoteBook } from "../code/NoteBook.js";
import { UserUtil } from "../auth/UserUtil.js";
import { Transformation } from "../node-types/Transformation.js";
import { LogDisplay } from "../log/LogDisplay.js";
import { Assets } from "../../../@still/util/componentUtil.js";
import { Header } from "../parts/Header.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";

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

	/** @Proxy @type { LogDisplay }*/
	logProxy;

	/** @Proxy @type { Header }*/
	headerProxy;

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

	/** @Prop */ showSaveButton = true;

	/** @Prop */ isAnyDiagramActive = false;

	/** @Prop */ wasDiagramSaved = false;

	/** @Prop */ selectedPplineName = false;

	/** @Prop */ openAgent = false;

	selectedLeftTab = 'content-diagram';

	/** @Prop */ schedulePeriodicitySelected;

	schedulePeriodicity;
	scheduleTimeType = 'min';
	scheduleTime;

	stOnRender() {
		AppTemplate.hideLoading();
		setTimeout(async () => {
			await Assets.import({ path: 'https://cdn.jsdelivr.net/npm/chart.js', type: 'js' });
		});

		this.service.on('load', () => {
			this.objectTypes = this.service.objectTypes;
			this.service.table.onChange(newValue => {
				console.log(`Workspace was update about changed and new value is: `, newValue);
			});
		});

		this.userService.on('load', async () => {
			let user = (await this.userService.getLoggedUser());

			if (user?.user)
				user = user?.user;

			if (UserUtil.name === null) {
				UserUtil.name = user.name;
				UserUtil.email = user.email;
				Object.freeze(UserUtil);
			}
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
			})*/
		});
		this.buildWorkspaceView();

		setTimeout(() => this.showLoading = false, 100);
		this.onLeftTabChange();
		this.handlePplineSchedulePopup();
		this.resetWorkspace();
	}

	onLeftTabChange() {
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
			this.controller.wSpaceComponent = this;
			this.editor.reroute = true;
			this.editor.start();
			this.controller.handleListeners(this.editor);
		});
	}

	loadDiagram(content) {
		this.editor.start();
		this.editor.import(content);
	}

	async savePipeline() {

		if (!this.controller.isTherePipelineToSave()) return null;

		if (this.activeGrid.value === 'Enter pipeline name')
			return AppTemplate.toast.error('Please enter a valid pipeline name');

		if (this.wasDiagramSaved)
			return this.controller.twiceDiagramSaveAlert('save');

		const data = await this.preparePipelineContent();
		if (data === null) return data;
		this.logProxy.showLogs = true;
		const result = await this.pplService.createOrUpdatePipeline(data);
		this.wasDiagramSaved = true;
		return result;
	}

	async updatePipeline() {

		if (!this.controller.isTherePipelineToSave()) return null;

		if (this.wasDiagramSaved)
			return this.controller.twiceDiagramSaveAlert('update');

		const isUpdate = true;
		const data = await this.preparePipelineContent(isUpdate);
		const result = await this.pplService.createOrUpdatePipeline(data, isUpdate);
		this.wasDiagramSaved = true;
		return result;
	}

	async preparePipelineContent(update = false) {

		this.controller.pplineStatus = PPLineStatEnum.Start;
		const formReferences = [...this.controller.formReferences.values()];
		let validationResults = formReferences.map(async (r) => {
			const component = Components.ref(r);

			if (component.getName() === SqlDBComponent.name) component.getTables();
			if (component.getName() === Transformation.name) {
				component.parseTransformationCode();
				return true;
			}

			const form = component.formRef;
			return await form?.validate();
		});

		validationResults = await Promise.all(validationResults);

		const anyInvalidForm = validationResults.indexOf(false) >= 0;
		const isValidSubmission = this.handleSubmissionError(anyInvalidForm);
		if (!isValidSubmission) return null;

		let data = this.editor.export();
		const startNode = this.controller.edgeTypeAdded[NodeTypeEnum.START];
		const activeGrid = this.activeGrid.value.toLowerCase().replace(/\s/g, '_');
		data = { ...data, user: this.userEmail, startNode, activeGrid, pplineLbl: this.activeGrid.value, socketSid: this.socketData.sid }
		console.log(data);

		if (update === true) data.update = true;

		return data;

	}

	resetWorkspace() {
		this.editor.clearModuleSelected();
		this.controller.resetEdges();
		document.querySelector('.clear-workspace-btn').style.right = '95px';
		this.activeGrid = 'Enter pipeline name';
		document.getElementById('pplineNamePlaceHolder').contentEditable = true;
		if (this.showSaveButton !== true)
			this.showSaveButton = true;
		this.wasDiagramSaved = false;
		this.isAnyDiagramActive = false;
		this.isAnyDiagramActive = false;
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

	onSplitterMove(params) {
		// 65 is the minimun height set on the template (Workspace.html)
		if (params.bottomHeight <= 65) this.isEditorOpened = false;
		else this.isEditorOpened = true;
		const editorHeight = ((params.bottomHeight) * 50) / 100;
		this.cmProxy.setHeight(editorHeight);
		this.terminalProxy.resizeHeight(editorHeight);
	}

	async runCode() {
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

	selecteLang(lang) {
		const langs = document.querySelectorAll('.editor-lang');
		this.cmProxy.changeLanguage(lang);
		this.editorActiveLang = lang;
		langs.forEach(elm => {
			if (elm.classList.contains(lang)) {
				elm.classList.remove(this.noSelectedLangClass);
				elm.classList.add(this.selectedLangClass);
			} else {
				elm.classList.add(this.noSelectedLangClass);
				elm.classList.remove(this.selectedLangClass);
			}
		})
	}

	async connectToDatabase(event, dbName) {

		event.preventDefault();
		const element = event.target;
		const database = dbName;
		const session = this.socketData.sid;
		const payload = { session, database };
		let result;

		if (this.connectedDbs.has(dbName)) {
			result = await (
				await this.service.handleDuckdbConnect(payload, WorkspaceService.DISCONECT_DB)
			).json();

			if (result.status) {
				this.connectedDbs.delete(dbName);
				element.style.color = 'grey';
			}

		} else {
			result = await (await this.service.handleDuckdbConnect(payload)).json();
			if (result.status) {
				this.connectedDbs.add(dbName);
				element.style.color = 'green';
			}
		}

		if (!result.status) AppTemplate.toast.error(result.message);

	}

	genInitialDBQuery(table, dbfile) {
		this.selecteLang('sql-lang');
		this.cmProxy.setCode(`use ${dbfile};\n\nSELECT * FROM ${table};`);
		this.codeEditorSplitter.setMaxHeight();
		this.isEditorOpened = true;
	}

	showHideEditor() {
		if (this.isEditorOpened) {
			// 88 Is because we're taking into account the splitter height itself 
			this.codeEditorSplitter.setHeight(88);
			this.isEditorOpened = false;
		} else {
			this.codeEditorSplitter.setMaxHeight();
			this.isEditorOpened = true;
		}
	}

	async viewPipelineDiagram(event, pplineName) {
		event.preventDefault();
		if (this.isAnyDiagramActive || this.controller.currentTotalNodes() > 0)
			return this.controller.moreThanOnePipelineOpenAlert();

		const response = await this.service.readDiagramFile(this.userEmail, pplineName);
		const result = JSON.parse(response);
		this.activeGrid = result.pipeline_lbl;
		document.querySelector('.clear-workspace-btn').style.right = '110px';
		this.showSaveButton = false;
		this.isAnyDiagramActive = true;
		document.getElementById('pplineNamePlaceHolder').contentEditable = false;
		await this.controller.processImportingNodes(result.content['Home'].data);
		this.wasDiagramSaved = false;
		this.selectedPplineName = pplineName;
	}

	async logout() {
		await this.userService.logOut();
	}

	verticalResize({ leftWidth }) {
		const selectedTab = this.selectedLeftTab.value;
		document.getElementsByClassName(selectedTab)[0].style.width = (leftWidth + 100) + 'px';
	}

	async viewScriptOnEditor() {
		const fileName = this.leftMenuProxy.scriptListProxy.selectedFile;
		const code = await this.service.readScriptFile(this.userEmail, fileName);
		this.noteBookProxy.openFile = { fileName, code };
		this.noteBookProxy.showNotebook = true;
		this.showDrawFlow = false;
	}

	viewFileOnEditor() {
		this.leftMenuProxy.scriptListProxy.selectedFile;
		//console.log(`WHEN CALLING FROM FILE: `,this.leftMenuProxy.fileListProxy.selectedFile);
	}

	handlePplineSchedulePopup() {
		const btnPipelineSchedule = document.getElementById('btnPipelineSchedule');

		this.schedulePeriodicity.onChange(val => {
			btnPipelineSchedule.disabled = true;
			this.schedulePeriodicitySelected = val, this.scheduleTime = '';
			handleBtnEnabling(this);
		});

		this.scheduleTimeType.onChange(() => handleBtnEnabling(this));
		this.scheduleTime.onChange(() => handleBtnEnabling(this));

		function handleBtnEnabling(obj = this) {
			if (obj.schedulePeriodicity.value === 'every') {
				if (['min', 'hour'].includes(obj.scheduleTimeType.value) && obj.scheduleTime.value !== "") {
					btnPipelineSchedule.disabled = false;
				} else btnPipelineSchedule.disabled = true;
			} else {
				if (obj.scheduleTime.value !== '') btnPipelineSchedule.disabled = false;
				else btnPipelineSchedule.disabled = true;
			}
		}

	}

	changeScheduleTime = (newValue) => this.scheduleTime = newValue;

	async scheduleJob() {

		const btnPipelineSchedule = document.getElementById('btnPipelineSchedule');
		btnPipelineSchedule.disabled = true;
		const payload = {
			ppline_name: this.selectedPplineName,
			socket_id: this.socketData.sid,
			settings: {
				type: this.scheduleTimeType.value,
				periodicity: this.schedulePeriodicity.value,
				time: this.scheduleTime.value,
				ppline_label: this.activeGrid.value,
			}
		};
		const result = await this.service.schedulePipeline(JSON.stringify(payload));
		if ([false, 'failed'].includes(result))
			AppTemplate.toast.error('Error while scheduling job for ' + this.activeGrid.value);
		else
			AppTemplate.toast.success('New schedule for ' + this.activeGrid.value + ' created successfully');

		const response = await WorkspaceService.getPipelineSchedules();
		this.headerProxy.scheduledPipelines = response.data;
		this.schedulePeriodicity = '';
		this.scheduleTime = '';
		btnPipelineSchedule.disabled = false;
	}

	showOrHideAgent = () => this.openAgent = !this.openAgent;

	async expandDataTableView() {

		//const { fields, data, query } = this.controller.aiAgentExpandView;
		const { fields, data, query } = mockData();
		console.log({ fields, data, query });
		const parsedFields = fields.replaceAll('\n', '').split(',').map(field => field.trim());
		
		const { template: gridUI } = await Components.new('Grid', { fields: parsedFields, data }, this.cmpInternalId);

		const { template: sqlEditorUI } = await Components.new('SqlEditor',
			{ query, fields: parsedFields, data }, this.cmpInternalId
		);

		const contentContainer = document
			.getElementById(this.popupWindowProxy.uniqueId)
			.querySelector('.popup-mov-window-content');

		const table = contentContainer.querySelector('.table-container');
		const codeEditor = contentContainer.querySelector('.code-editor-container');

		table.innerHTML = gridUI;
		codeEditor.innerHTML = sqlEditorUI;

		this.popupWindowProxy.openPopup();
	}

}


function mockData() {

	return {
		"fields": " _dlt_id, _dlt_load_id, industry_aggregation_nzsioc, industry_code_anzsic06, industry_code_nzsioc, industry_name_nzsioc, units, value, variable_category, variable_code, variable_name, year ",
		"data": [
			[
				"60+EtKt5eyv/Vg",
				"1759444516.252254",
				"LEVEL 1",
				"ANZSIC06 divisions A-S (excluding classes K6330, L6711, O7552, O760, O771, O772, S9540, S9601, S9602, and S9603)",
				"99999",
				"All industries",
				"Dollars (millions)",
				"979594",
				"Financial performance",
				"H01",
				"Total income",
				2024
			],
			[
				"fAaqq1yvWm4f2Q",
				"1759444516.252254",
				"LEVEL 1",
				"ANZSIC06 divisions A-S (excluding classes K6330, L6711, O7552, O760, O771, O772, S9540, S9601, S9602, and S9603)",
				"99999",
				"All industries",
				"Dollars (millions)",
				"838626",
				"Financial performance",
				"H04",
				"Sales, government funding, grants and subsidies",
				2024
			],
			[
				"1A/NBuoygzQ5GQ",
				"1759444516.252254",
				"LEVEL 1",
				"ANZSIC06 divisions A-S (excluding classes K6330, L6711, O7552, O760, O771, O772, S9540, S9601, S9602, and S9603)",
				"99999",
				"All industries",
				"Dollars (millions)",
				"112188",
				"Financial performance",
				"H05",
				"Interest, dividends and donations",
				2024
			],
			[
				"gyFWz0QeeN+sPg",
				"1759444516.252254",
				"LEVEL 1",
				"ANZSIC06 divisions A-S (excluding classes K6330, L6711, O7552, O760, O771, O772, S9540, S9601, S9602, and S9603)",
				"99999",
				"All industries",
				"Dollars (millions)",
				"28781",
				"Financial performance",
				"H07",
				"Non-operating income",
				2024
			],
			[
				"3FqdtYIhZY1STg",
				"1759444516.252254",
				"LEVEL 1",
				"ANZSIC06 divisions A-S (excluding classes K6330, L6711, O7552, O760, O771, O772, S9540, S9601, S9602, and S9603)",
				"99999",
				"All industries",
				"Dollars (millions)",
				"856960",
				"Financial performance",
				"H08",
				"Total expenditure",
				2024
			],
			[
				"2OE/jBRZVGsVbQ",
				"1759444516.252254",
				"LEVEL 1",
				"ANZSIC06 divisions A-S (excluding classes K6330, L6711, O7552, O760, O771, O772, S9540, S9601, S9602, and S9603)",
				"99999",
				"All industries",
				"Dollars (millions)",
				"71493",
				"Financial performance",
				"H09",
				"Interest and donations",
				2024
			],
			[
				"FePXd8ygEYwlHQ",
				"1759444516.252254",
				"LEVEL 1",
				"ANZSIC06 divisions A-S (excluding classes K6330, L6711, O7552, O760, O771, O772, S9540, S9601, S9602, and S9603)",
				"99999",
				"All industries",
				"Dollars (millions)",
				"8540",
				"Financial performance",
				"H10",
				"Indirect taxes",
				2024
			],
			[
				"WmkgNE6TIUMG5g",
				"1759444516.252254",
				"LEVEL 1",
				"ANZSIC06 divisions A-S (excluding classes K6330, L6711, O7552, O760, O771, O772, S9540, S9601, S9602, and S9603)",
				"99999",
				"All industries",
				"Dollars (millions)",
				"32896",
				"Financial performance",
				"H11",
				"Depreciation",
				2024
			],
			[
				"Y//o/z91bSNRPA",
				"1759444516.252254",
				"LEVEL 1",
				"ANZSIC06 divisions A-S (excluding classes K6330, L6711, O7552, O760, O771, O772, S9540, S9601, S9602, and S9603)",
				"99999",
				"All industries",
				"Dollars (millions)",
				"157616",
				"Financial performance",
				"H12",
				"Salaries and wages paid",
				2024
			],
			[
				"wNJlpjm1ndsH/Q",
				"1759444516.252254",
				"LEVEL 1",
				"ANZSIC06 divisions A-S (excluding classes K6330, L6711, O7552, O760, O771, O772, S9540, S9601, S9602, and S9603)",
				"99999",
				"All industries",
				"Dollars (millions)",
				"323",
				"Financial performance",
				"H13",
				"Redundancy and severance",
				2024
			]
		],
		"query": "SELECT\n_dlt_id, _dlt_load_id, industry_aggregation_nzsioc, industry_code_anzsic06,\nindustry_code_nzsioc, industry_name_nzsioc, units, value,\nvariable_category, variable_code, variable_name, year\nFROM myserveydb.tabelaservey\nLIMIT 10"
	}

}