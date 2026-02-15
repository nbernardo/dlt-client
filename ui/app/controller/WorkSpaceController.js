import { BaseController } from "../../@still/component/super/service/BaseController.js";
import { Components } from "../../@still/setup/components.js";
import { Assets } from "../../@still/util/componentUtil.js";
import { StillAppSetup } from "../../config/app-setup.js";
import { AppTemplate } from "../../config/app-template.js";
import { AIAgent } from "../components/agent/AIAgent.js";
import { CatalogForm } from "../components/catalog/CatalogForm.js";
import { LeftTabs } from "../components/navigation/left/LeftTabs.js";
import { NodeTypeInterface } from "../components/node-types/mixin/NodeTypeInterface.js";
import { Transformation } from "../components/node-types/Transformation.js";
import { Header } from "../components/parts/Header.js";
import { Workspace } from "../components/workspace/Workspace.js";
import { CodeEditorUtil } from "../util/CodeEditorUtil.js";

const monacoLoadInitVal = { 
    lang: 'sql', 
    fontSize: 14, 
    theme: 'vs-light', 
    suggestions: [],
    suggestionType: null 
};

class NodeType {
    tmplt; data;
}

export const NodeTypeEnum = {
    START: 'Start',
    END: 'End',
}

export const PPLineStatEnum = {
    Start: 'Start',
    Progress: 'Progress',
    Finished: 'Finished',
    Failed: 'Failed',
}

export class AIAgentExpandViewType {
    fields = '';
    query = '';
    /** @type { Array<Array<Object>> } */
    data;
    initialTable = '';
    database = null
}

export class WorkSpaceController extends BaseController {

    editor;
    transform = '';
    edgeTypeAdded = {};
    formReferences = new Map();
    validationErrors = [];
    idCounter = 0;
    cmpIdToNodeIdMap = {};
    /** @type { {[string]: Set }  } */
    pplineSteps = {};
    /** @type { PPLineStatEnum } */
    pplineStatus;
    /** @type { Workspace } */
    wSpaceComponent;
    isImportProgress = false;
    isSubmittingPipeline = false;
    static isS3AuthTemplate = false;

    importingPipelineSourceDetails = null;

    /** @type { Header } */ activeHeader = null;
    /** @type { Header } */ isDuckDBDest = false;

    static scheduledPipelinesInitList;

    /** Pipeline schedule variables */
    btnPipelineSchedule;
    dropMenu;

    pipelineDestinationTrace = { sql: {}, duckdb: {} };

    /** 
     * When loading/importing an existing diagram 
     * the nodes are re-created in the diagram as 
     * needed due to that the nodeId can be different,
     * hence we have the bellow map  
     * */
    static importNodeIdMapping;

    /** @type { AIAgentExpandViewType } */
    aiAgentExpandView = {};

    /** @type { Map<Number, AIAgentExpandViewType> } */
    aiAgentExpandViewMap = new Map();

    aiAgentExpandViewCount = 0;

    /** @type { LeftTabs } */
    leftTab;

    constructor(){
        super();
        (async () => await this.loadMonacoEditorDependencies())();
    }

    pipelineCreateMetadata(){
        return { 
            s3Auth: WorkSpaceController.isS3AuthTemplate
        }
    }

    pipelineMetadataClear(){
        WorkSpaceController.isS3AuthTemplate = undefined;
    }

    resetEdges() {
        this.edgeTypeAdded = {};
        this.formReferences.clear();
        this.validationErrors = [];
        this.idCounter = 0;
        this.cmpIdToNodeIdMap = {};
        this.pplineStatus = {};
        this.editor.nodeId = 1;
        this.pipelineDestinationTrace = { sql: {}, duckdb: {} }
        this.nodeIdToComponentIdMap = {}
        this.drawnNodeList = [];
        this.drawnNodes = 0;
        this.importingPipelineSourceDetails = null;
        this.isTemplating = false;
    }

    /** @param { AIAgentExpandViewType } aiAgentExpandView */
    addAIAgentGridExpand(id){
        const obj = JSON.parse(JSON.stringify(this.aiAgentExpandView));
        this.aiAgentExpandViewMap.set(id, obj);
    }

    /** @returns { AIAgentExpandViewType } */
    getAIAgentGridExpand(id){
        const result = this.aiAgentExpandViewMap.get(id);
        if(!result)
            return { fields: undefined, data: undefined, query: undefined, database: undefined };
        return result;
    }

    registerEvents() {
        let mobile_item_selec = '';
        let mobile_last_move = null;
        window.drawpositionMobile = (ev) => mobile_last_move = ev;
    }

    allowDrop = (ev) => ev.preventDefault();

    showpopup(e) {
        e.target.closest(".drawflow-node").style.zIndex = "9999";
        e.target.children[0].style.display = "block";

        this.transform = this.editor.precanvas.style.transform;
        this.editor.precanvas.style.transform = '';
        this.editor.precanvas.style.left = this.editor.canvas_x + 'px';
        this.editor.precanvas.style.top = this.editor.canvas_y + 'px';
        console.log(transform);
        this.editor.editor_mode = "fixed";
    }

    closemodal(e) {
        e.target.closest(".drawflow-node").style.zIndex = "2";
        e.target.parentElement.parentElement.style.display = "none";

        this.editor.precanvas.style.transform = this.transform;
        this.editor.precanvas.style.left = '0px';
        this.editor.precanvas.style.top = '0px';
        this.editor.editor_mode = "edit";
    }

    drag(ev, disabled, isNoteGragabble) {

        if(isNoteGragabble == 'yes') return;

        if(disabled === 'yes'){
            return this.showDialog(
                'This node type is not yet available', 
                { type: 'ok', title: 'Unavailable feature' }
            );
        }
        if (ev.type === "touchstart") {
            this.mobileItemSelec = ev.target.closest(".drag-drawflow").getAttribute('data-node');
        } else {
            const icon = ev.target.getAttribute('data-icon');
            const img = ev.target.getAttribute('data-img');

            const isIconValid = !['undefined', 'null', null, ''].includes(icon);
            ev.dataTransfer.setData("node", ev.target.getAttribute('data-node'));
            ev.dataTransfer.setData("label", ev.target.getAttribute('data-lbl'));
            ev.dataTransfer.setData("source", ev.target.getAttribute('data-src'));
            ev.dataTransfer.setData("dest", ev.target.getAttribute('data-dst'));
            ev.dataTransfer.setData("template", ev.target.getAttribute('data-template'));
            ev.dataTransfer.setData("icon", isIconValid ? icon : null);
            ev.dataTransfer.setData("img", isIconValid ? null : img);
        }
    }

    async drop(ev) {
        if (ev.type === "touchend") {
            var parentdrawflow = document.elementFromPoint(mobile_last_move.touches[0].clientX, mobile_last_move.touches[0].clientY).closest("#drawflow");
            if (parentdrawflow != null) {
                await this.addNodeToDrawFlow(this.mobileItemSelec, mobile_last_move.touches[0].clientX, mobile_last_move.touches[0].clientY);
            }
            this.mobileItemSelec = '';
        } else {
            ev.preventDefault();
            const name = ev.dataTransfer.getData("node");
            const label = ev.dataTransfer.getData('label');
            const icon = ev.dataTransfer.getData('icon');
            const img = ev.dataTransfer.getData('img');
            const source = ev.dataTransfer.getData("source");
            const dest = ev.dataTransfer.getData("dest");
            const template = ev.dataTransfer.getData("template");
            
            const [pos_x, pos_y] = [ev.clientX, ev.clientY];
            const data = { name, label, icon, img, pos_x, pos_y, source, dest, template };

            await this.addNodeToDrawFlow(data);
        }
    }

    async addNodeToDrawFlow({ name, label, icon, img, pos_x, pos_y, source, dest, template }) {
        const inType = name;
        if (this.editor.editor_mode === 'fixed') return false;
        if (['bucket', 'slack', 'log'].includes(name)) name = 'generic';

        pos_x = pos_x * (this.editor.precanvas.clientWidth / (this.editor.precanvas.clientWidth * this.editor.zoom)) - (this.editor.precanvas.getBoundingClientRect().x * (this.editor.precanvas.clientWidth / (this.editor.precanvas.clientWidth * this.editor.zoom)));
        pos_y = pos_y * (this.editor.precanvas.clientHeight / (this.editor.precanvas.clientHeight * this.editor.zoom)) - (this.editor.precanvas.getBoundingClientRect().y * (this.editor.precanvas.clientHeight / (this.editor.precanvas.clientHeight * this.editor.zoom)));

        if (['Start', 'End'].includes(inType))
            return this.addStartOrEndNode(name, source, dest, pos_x, pos_y);

        const nodeId = this.getNodeId();
        this.isImportProgress = false;
        const parentId = this.wSpaceComponent.cmpInternalId;
        const { template: tmpl, component } = await Components.new(inType, { nodeId, isImport: false, template }, parentId);
        this.handleAddNode(component, nodeId, name, pos_x, pos_y, tmpl);

    }

    drawnNodes = 0;
    drawnNodeList = [];
    currentNodePositionX;
    prevAiDrawnNode;
    drawnStartNode 

    async createNode(type, data){

        const add = this.drawnNodes > 1 ? 10 : 0;
        const addToNodePosx = this.prevAiDrawnNode == Transformation.name ? 1.15 : 0;
        const pos_x = (307 * ((this.drawnNodes++) + addToNodePosx)) + add, pos_y = (183);
        const nodeId = this.getNodeId();
        this.prevAiDrawnNode = type;

        if(type === NodeTypeEnum.START){
            let source = false, dest = true;
            this.addStartOrEndNode(type, source, dest, pos_x, pos_y);
            this.edgeTypeAdded[NodeTypeEnum.START] = nodeId;
        }else{
            const parentId = this.wSpaceComponent.cmpInternalId;
            const { template: tmpl, component } = await Components.new(type, { ...data, aiGenerated: true, nodeId }, parentId);
            this.currentNodePositionX = pos_x;
            await this.handleAddNode(component, nodeId, type, pos_x, pos_y, tmpl);
        }
        this.drawnNodeList.push(nodeId);

    }

    async linkAgentCreatedNodes(totalNodes){

        const pos_x = 100, pos_y = 40;
        this.addStartOrEndNode(NodeTypeEnum.START, false, true, pos_x, pos_y);
        this.edgeTypeAdded[NodeTypeEnum.START] = totalNodes + 1;

        let currentNode = this.drawnNodeList.shift();
        this.editor.addConnection((totalNodes + 1), currentNode, 'output_1', 'input_1');

        let nextNode = this.drawnNodeList[0];
        this.editor.addConnection(currentNode, nextNode, 'output_1', 'input_1');

        while(this.drawnNodeList.length){
            currentNode = this.drawnNodeList.shift();
            nextNode = this.drawnNodeList[0];
            this.editor.addConnection(currentNode, nextNode, 'output_1', 'input_1');
        }
        
    }

    /** @returns { WorkSpaceController } */
    static instance(){
        return WorkSpaceController.get();
    }

    isTemplating = false;
    async processImportingNodes(nodeData, asTemplate = false) {

        WorkSpaceController.importNodeIdMapping = {};
        const [inOutputMapping, nodeList] = [{}, Object.entries(nodeData)];
        let nodeId = 0;
        this.isImportProgress = nodeList.length > 0 ? true : false, this.idCounter = 0;

        for (let [originalNodeId, { class: name, data, pos_x, pos_y, source, dest, inputs, outputs }] of nodeList) {
            
            nodeId++;
            WorkSpaceController.importNodeIdMapping[originalNodeId] = Number(nodeId)
            
            if (!['Start', 'End'].includes(name)) {
                //The extracted fields and nodeId are the fields inside the components itself ( from node-types folder )
                let { componentId: removedId, ...fields } = data;
                const parentId = this.wSpaceComponent.cmpInternalId;
                const { template: tmpl, component } = await Components.new(name, { nodeId, ...fields, isImport: true, asTemplate }, parentId);

                this.handleAddNode(component, nodeId, name, pos_x, pos_y, tmpl);
                setTimeout(() => Object.keys(fields).forEach((f) => component[f] = fields[f]), 10);

                //We first collects all links to render after all nodes are in the diagram
                inOutputMapping[nodeId] = { inputs, outputs };

            } else {
                [source, dest] = [name === 'End', name === 'Start'];
                this.addStartOrEndNode(name, source, dest, pos_x, pos_y);
                inOutputMapping[nodeId] = { inputs, outputs };
                if(name === 'Start')
                    this.edgeTypeAdded[NodeTypeEnum.START] = nodeId;
            }
        }
        this.idCounter = nodeId;
        setTimeout(() => {
            Object.keys(inOutputMapping).forEach(nodeId => {
                const { outputs } = inOutputMapping[nodeId];
                outputs?.output_1?.connections.forEach((link) => {
                    const targetNode = WorkSpaceController.importNodeIdMapping[Number(link.node)];
                    this.editor.addConnection(Number(nodeId), targetNode, 'output_1', 'input_1');
                });
            });
        },100);
        this.isTemplating = asTemplate;
    }

    addStartOrEndNode(name, source, dest, pos_x, pos_y) {
        const tmpl = this.edgeType(name);
        if (tmpl != null)
            return this.editor.addNode(name, source, dest, pos_x, pos_y, name, {}, tmpl);
        return;
    }

    handleAddNode(component, nodeId, name, pos_x, pos_y, tmpl) {
        this.isTemplating = true;
        const { inConnectors, outConnectors } = component;
        const initData = { componentId: component.cmpInternalId };
        this.formReferences.set(nodeId, component.cmpInternalId);
        this.edgeTypeAdded[nodeId] = new Set();
        const node = this.editor.addNode(name, inConnectors, outConnectors, pos_x, pos_y, name, initData, tmpl);
        this.clearHTML(nodeId);
        this.cmpIdToNodeIdMap[component.cmpInternalId] = nodeId;
        return node;
    }

    changeMode(option) {
        if (option == 'lock') {
            lock.style.display = 'none';
            unlock.style.display = 'block';
        } else {
            lock.style.display = 'block';
            unlock.style.display = 'none';
        }
    }

    changeModule(event) {
        const all = document.querySelectorAll(".menu ul li");
        for (var i = 0; i < all.length; i++) {
            all[i].classList.remove('selected');
        }
        event.target?.classList?.add('selected');
    }


    /** @returns { NodeType } */
    edgeType(type) {
        //Skip if already added
        if (this.edgeTypeAdded[type]) return null;
        this.edgeTypeAdded[type] = this.getNodeId();
        return `
            <div class="edge-label">${type}</div>
            <div></div>
        `;
    }

    checkStartNode() {
        return this.edgeTypeAdded[NodeTypeEnum.START];
    }

    events() {
        return {
            drag: window.drawdrag,
            drop: window.drawdrop,
            positionMobile: window.drawpositionMobile
        }
    }

    isStartOrEndNode(id) {
        return this.edgeTypeAdded[NodeTypeEnum.START] == id
            || this.edgeTypeAdded[NodeTypeEnum.END] == id;
    }

    nodeIdToComponentIdMap = {};
    handleListeners(editor) {

        // Events!
        const obj = this;
        editor.on('nodeCreated', function (id) {
            console.log("Node created " + id);
            obj.nodeIdToComponentIdMap[id] = WorkSpaceController.getNode(id).data.componentId;
        });

        editor.on('nodeRemoved', function (id) {

            Components.ref(obj.nodeIdToComponentIdMap[id]).stOnUnload();
            delete obj.nodeIdToComponentIdMap[id];
            
            if(obj.currentTotalNodes() === 0) obj.wSpaceComponent.resetWorkspace();
            
            if (id == obj.edgeTypeAdded[NodeTypeEnum.START])
                return delete obj.edgeTypeAdded[NodeTypeEnum.START];

            if (id == obj.edgeTypeAdded[NodeTypeEnum.END])
                return delete obj.edgeTypeAdded[NodeTypeEnum.END];

            //Remove the form so it does not gets considered
            //when validating the pipeline submittions/save
            obj.formReferences.delete(Number(id));
            delete obj.edgeTypeAdded[id];
        });

        editor.on('nodeSelected', function (id) {
            console.log("Node selected " + id);
        });

        editor.on('moduleCreated', function (name) {
            console.log("Module Created " + name);
        });

        editor.on('moduleChanged', function (name) {
            console.log("Module Changed " + name);
        });

        editor.on('connectionCreated', connections => obj.onConnectionCreate(connections, obj));

        editor.on('connectionRemoved', function (connection) {
            const { output_id, input_id } = connection;
            if (output_id != obj.edgeTypeAdded[NodeTypeEnum.START])
                obj.edgeTypeAdded[output_id].delete(input_id);

            if (input_id != obj.edgeTypeAdded[NodeTypeEnum.END])
                obj.edgeTypeAdded[input_id].delete(output_id);

            const { data: nodeIn } = WorkSpaceController.getNode(input_id);
            const { data: nodeOut } = WorkSpaceController.getNode(output_id);

            const destCmpId = nodeIn.componentId, srcCmpId = nodeOut.componentId;

            const /** @type { NodeTypeInterface } */ destCmp = Components.ref(destCmpId) || {};
            const /** @type { NodeTypeInterface } */ srcCmp = Components.ref(srcCmpId) || {};
            
            const sourceType = ('getName' in srcCmp) ? srcCmp.getName() : 'Start/End node';
            if ('onConectionDelete' in destCmp) destCmp.onConectionDelete(sourceType);

        });

        editor.on('mouseMove', function (position) {
            //console.log('Position mouse x:' + position.x + ' y:' + position.y);
        });

        editor.on('nodeMoved', function (id) {
            console.log("Node moved " + id);
        });

        editor.on('zoom', function (zoom) {
            console.log('Zoom level ' + zoom);
        });

        editor.on('translate', function (position) {
            console.log('Translate x:' + position.x + ' y:' + position.y);
        });

        editor.on('addReroute', function (id) {
            console.log("Reroute added " + id);
        });

        editor.on('removeReroute', function (id) {
            console.log("Reroute removed " + id);
        });

    }

    getNodeId() {
        this.idCounter = this.idCounter + 1
        return this.idCounter;
    }

    static getNode(nodeId) {
        const obj = WorkSpaceController.get();
        return obj.editor.drawflow.drawflow.Home.data[nodeId] || {};
    }

    currentTotalNodes() {
        const obj = WorkSpaceController.get();
        return Object.keys(obj.editor.drawflow.drawflow.Home.data)?.length || 0;
    }

    clearHTML(nodeId) {
        WorkSpaceController.getNode(nodeId).html = '';
    }


    export() {
        const exportResult = this.editor.export();
        console.log(JSON.stringify(exportResult, null, 4));
    }

    pipelineSuccess = true;
    socketChannelSetup(io, socketData) {

        const wssAddr = StillAppSetup.config.get('websocketAddr');
        const socket = io(wssAddr, { 
            transports: ["websocket"] 
        });

        socket.on('connect', () => { });
        socket.on('connected', async (data) => {
            socketData.sid = data.sid;
            await this.wSpaceComponent.service.updateSocketId(data.sid);
        });
        //This will be used to revert pipeline
        socket.on('pplineError', ({ componentId, sid, error }) => {
            WorkSpaceController.addFailedStatus(componentId);
            AppTemplate.toast.error(error.message);
            this.wSpaceComponent.logProxy.lastLogTime = null; //Reset the logging time
            this.wSpaceComponent.wasDiagramSaved = false;
            this.pipelineSuccess = false;
        });

        socket.on('pplineStepStart', ({ componentId, sid }) => {
            if (!this.pplineSteps[sid]) this.pplineSteps[sid] = new Set();
            this.pplineSteps[sid].add(componentId);
            WorkSpaceController.addRunningStatus(componentId);
        });

        socket.on('pplineStepSuccess', ({ componentId, sid }) => {
            if (!this.pplineSteps[sid]) this.pplineSteps[sid] = new Set();
            this.pplineSteps[sid].add(componentId);
            const nodeId = this.cmpIdToNodeIdMap[componentId];
            const node = WorkSpaceController.getNode(nodeId);
            this.pipelineSuccess = true;
            if (Object.keys(node.outputs).length > 0)
                WorkSpaceController.addPreSuccessStatus(componentId);
            this.wSpaceComponent.logProxy.lastLogTime = null; //Reset the logging time
        });

        socket.on('pplineSuccess', ({ sid }) => {
            const tasks = this.pplineSteps[sid];
            this.pplineStatus = PPLineStatEnum.Finished;
            if(tasks)
                [...tasks].forEach(WorkSpaceController.addSuccessStatus);
        });

        socket.on('pplineTrace', ({ data: trace, time, error, job, warn }) => {
            const logType = warn ? 'warn' : (error ? 'error' : 'info');

            // Because the backend is running in multithreading, sometimes multiple thread are trying to access 
            // the database file (.duckdb), which does not harm the normal proces, anyway an exceptio can be thrown
            // hence we skip it by checking if it happenes when it comes to pipeline job that are running
            if(job && trace.indexOf('Could not set lock on file') > 0)
                return;
            this.wSpaceComponent.logProxy.appendLogEntry(logType, trace, time);
        });

    }

    static addRunningStatus(containerHTMLClass) {
        const container = document
            .querySelector(`.${containerHTMLClass}`);

        if(container){
            container.querySelector('div[class="title-box pipeline-node-type-title-box"]')
                .querySelector('.statusicon')
                .className = 'statusicon running-status';
        }
    }

    static remRunningStatus(containerHTMLClass) {
        const container = document
            .querySelector(`.${containerHTMLClass}`);
            
        if(container){
            container.querySelector('div[class="title-box pipeline-node-type-title-box"]')
                .querySelector('.statusicon')
                .classList.remove('running-status');
        }
    }

    static addFailedStatus(containerHTMLClass) {
        const container = document
            .querySelector(`.${containerHTMLClass}`);
            
        if(container){
            container.querySelector('div[class="title-box pipeline-node-type-title-box"]')
                .querySelector('.statusicon')
                .className = 'statusicon failed-status';
        }
    }

    static addSuccessStatus(containerHTMLClass) {
        const container = document
            .querySelector(`.${containerHTMLClass}`);
            
        if(container){
            container.querySelector('div[class="title-box pipeline-node-type-title-box"]')
                .querySelector('.statusicon')
                .className = 'statusicon success-status';
        }
    }

    static addPreSuccessStatus(containerHTMLClass) {
        const container = document
            .querySelector(`.${containerHTMLClass}`);
            
        if(container){
            container.querySelector('div[class="title-box pipeline-node-type-title-box"]')
                .querySelector('.statusicon')
                .className = 'statusicon pre-success-status';
        }
    }

    addWorkspaceError(error) {
        const elm = document.createElement('li');
        elm.innerText = error;
        this.validationErrors.push(elm);
    }

    isValidationError = () => this.validationErrors.length;
    clearValidationError = () => this.validationErrors = [];

    /** @returns { Array<HTMLElement> } */
    getInputsByNodeId(componentId) {
        return document
            .querySelector(`.${componentId}`)
            .parentNode.parentNode
            .querySelector('.inputs')
            .children
    }

    /** @param { WorkSpaceController } obj  */
    onConnectionCreate(connection, obj) {
        const { output_id, input_id } = connection;

        const { data: nodeIn } = WorkSpaceController.getNode(input_id);
        const { data: nodeOut } = WorkSpaceController.getNode(output_id);

        this.reactiveNotifyOnConnection(nodeOut, nodeIn);

        if (nodeIn?.componentId) {
            const inputs = obj.getInputsByNodeId(nodeIn?.componentId);
            [...inputs].forEach(el => {
                el.style.background = 'white';
                el.classList.remove('blink');
            });
        }
        // In case it's importing, reviweing the previous created pipeline
        // Not allowing changes other than the transformations
        if (obj.edgeTypeAdded[output_id] !== undefined) {
            if (output_id != obj.edgeTypeAdded[NodeTypeEnum.START])
                obj.edgeTypeAdded[output_id].add(input_id);

            if (input_id != obj.edgeTypeAdded[NodeTypeEnum.END])
                obj.edgeTypeAdded[input_id]?.add(output_id);
        }
    }

    /** This method if to provide the components of the workspace with capabilities to return something 
     *  to the component it's connecting connecting to, then the connecting component will implement 
     *  onOutputConnection and connected component will implement onInputConnection providing reactive
     *  communication when connecting one node to another */
    reactiveNotifyOnConnection(nodeOut, nodeIn) {

        const destCmpId = nodeIn.componentId, srcCmpId = nodeOut.componentId;

        const /** @type { NodeTypeInterface } */ destCmp = Components.ref(destCmpId) || {};
        const /** @type { NodeTypeInterface } */ srcCmp = Components.ref(srcCmpId) || {};

        if(Object.keys(nodeOut).length == 0)
            srcCmp.nodeName = 'Start';

        srcCmp.nextNode = destCmp;

        function setupNotification() {
            if ('onOutputConnection' in srcCmp || srcCmp.nodeName === 'Start') {
                if ('onInputConnection' in destCmp) {
                    (async () => {
                        const sourceData = srcCmp.nodeName === 'Start' ? {} : await srcCmp?.onOutputConnection();
                        await destCmp.onInputConnection({ data: sourceData, type: srcCmp?.nodeName === 'Start' ? 'Start' : srcCmp?.getName() });
                    })();
                }
            }
        }
        if(this.isImportProgress === false)
            return setupNotification();

        /** subscribeAction is the other side of emiAction boths under Components 
         *  in this case, we want the source component to notify targed component
         *  only when it emits an event with its id (source component id) stating */
        Components.subscribeAction(`nodeReady${srcCmpId}`, () => setupNotification());

    }

    copyToClipboard(content) {
        navigator.clipboard.writeText(content);
    }

    shouldDisableNodeFormInputs = true;

    disableNodeFormInputs(formWrapClass) {
        if(this.shouldDisableNodeFormInputs){
            const disable = true, defautlFields = 'input[type=text], select';
            // Disable the default form inputs
            document.querySelector('.' + formWrapClass).querySelectorAll(defautlFields).forEach(elm => {
                elm.disabled = disable;
            });
        }
    }

    /**
     * 
     * @param {*} message 
     * @param {{ type: 'confirm'|'ok', onConfirm: Function, onCancel: Function, title: String }} param1 
     */
    showDialog(message,{ type = 'confirm', onConfirm = () => {}, onCancel = () => {}, title = null } = {}){
        const dialog = document.getElementById('wodkspaceMainDialog');
        dialog.querySelector('.dialog-title').innerHTML = title || 'Confirm Action';
        const defaultMessage = dialog.querySelector('.dialog-message').innerHTML;

        if(type === 'confirm'){
            document.querySelector('.btn-confirm').style.display = '';
            document.querySelector('.btn-cancel').style.display = '';
            document.querySelector('.btn-ok').style.display = 'none';
        }

        if(type === 'ok'){
            document.querySelector('.btn-confirm').style.display = 'none';
            document.querySelector('.btn-cancel').style.display = 'none';
            document.querySelector('.btn-ok').style.display = '';          
        }

        if(message)
            dialog.querySelector('.dialog-message').innerHTML = message;
        
        dialog.showModal();

        dialog.querySelector('.btn-confirm').onclick = async () => await confirmAction();
        dialog.querySelector('.btn-ok').onclick = async () => await confirmAction();

        dialog.querySelector('.btn-cancel').onclick = async () => {
            await onCancel(); dialog.close();
            dialog.querySelector('.dialog-message').innerHTML = defaultMessage;
        }

        async function confirmAction(){
            await onConfirm(); dialog.close();
            dialog.querySelector('.dialog-message').innerHTML = defaultMessage;
        }
    }

    /**
     * @param {'save'|'update'} type 
     */
    twiceDiagramSaveAlert(type = 'save'){
		let message = 'You cannot load more than one pipelin at time, please clear the workspace to load another pipeline';
		let title = 'Cannot save the pipeline twice.';
        if(type === 'update'){
            title = 'No changes to update.';
            message = 'No changes were done on the diagram/pipeline, so there is nothing to update';
        }
		return this.showDialog(message, { type: 'ok', title });
    }
    
    moreThanOnePipelineOpenAlert(cb = () => {}){
        const message = 'You cannot load more than one pipelin at time, <br><b>Do you want to discard the existing diagram to render the clicked one?</b>';
		const title = 'Cannot load multiple pipeline.';
		return this.showDialog(message, { type: 'confirm', title, onConfirm: async () => await cb(true) });	
    }
    
    noPipelineToSaveAlert(){
        const message = 'There is no pipeline in the workspace to be save/updated';
		const title = 'Nothing to be saved.';
		return this.showDialog(message, { type: 'ok', title });	
    }
    
    clearActiveDiagramAlert(cb = () => {}){
        const message = 'You sure you want to clear the current diagram?';
		const title = 'Clearing active diagram.';
		return this.showDialog(message, { type: 'confirm', title, onConfirm: async () => await cb(true) });	
    }

    isTherePipelineToSave(){
        if(this.currentTotalNodes() == 0 && !this.wSpaceComponent.isAnyDiagramActive){
			this.noPipelineToSaveAlert();
			return false;
		}
        return true;
    }

    /** @returns { { btnPipelineSchedule: HTMLButtonElement } } */
    getPplineScheduleVars(){
        if(this.btnPipelineSchedule === undefined){
            this.btnPipelineSchedule = document.getElementById('btnPipelineSchedule');
            this.dropMenu = document.querySelector('.pipeline-context-drop-menu .schedule-drop-submenu');
        }
        return { btnPipelineSchedule: this.btnPipelineSchedule, dropMenu: this.dropMenu };
    }

    /** @param { HTMLDivElement } target */
    handlePplineScheduleHideShow(target){
        
        target.nextElementSibling.style.display = 'block';

        if(!target.dataset.eventSet){
            target.dataset.eventSet = true;
            const dropMenu = target.nextElementSibling;
            
		    document.addEventListener('mouseover', (event) => 
		    	!dropMenu.contains(event.target) && !target.contains(event.target)  ? dropMenu.style.display = 'none' : ''
		    );
        }
    }

    /** @type { AIAgent } */
    startedAgent = null;

    async startAgent(retry = false){ 
        if(!this.startedAgent){
            
            const totalMessages = this.wSpaceComponent.service.aiAgentNamespaceDetails.conversation_count;
            const messageCountLimit = this.wSpaceComponent.service.aiAgentNamespaceDetails.user_message_count_limit;

            const parentId = this.wSpaceComponent.cmpInternalId;
            const { template, component } = await Components.new(AIAgent, {totalMessages, messageCountLimit}, parentId);
            this.startedAgent = component;   
            document.querySelector('.ai-agent-placeholder').insertAdjacentHTML('beforeend', template);
            this.wSpaceComponent.showOrHideAgent();
        }else{
            if(!this.wSpaceComponent.openAgent)
                this.wSpaceComponent.showOrHideAgent();

            this.startedAgent.startNewAgent(retry);
        }
    }

    /** @type { CatalogForm } */
    catalogForm = null;
    
    async createCatalogForm(type = 1 /** 1 = db, 2 = api */){
        const parentId = this.wSpaceComponent.cmpInternalId;
        const { template: catalogFormUI, component } = await Components.new(CatalogForm, { type }, parentId);
        this.catalogForm = component;
        this.wSpaceComponent.dynamicViewPlaceholder.innerHTML = catalogFormUI;
    }

    async loadMonacoEditorDependencies(){
        
        if (window.monaco) return;

        await Assets.import({ path: 'https://cdn.jsdelivr.net/npm/showdown/dist/showdown.min.js' });
        await Assets.import({ path: 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.46.0/min/vs/loader.min.js' });

        require.config({ paths: { 'vs': 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.46.0/min/vs' } });
        require(['vs/editor/editor.main'], (monaco) => {

            monaco.languages.registerCompletionItemProvider('python', {
                provideCompletionItems: (model, position) => {
                    return { suggestions: CodeEditorUtil.getPythonSuggestions() };
                },
            });

            window.monaco
        });
    }

    loadMonacoEditor(container, params = monacoLoadInitVal){
		
        const { lang, fontSize, theme, suggestions, suggestionType } = params;
        if(suggestionType === 'secret') CodeEditorUtil.addSecretSugestion(lang, suggestions);

		return window.monaco.editor.create(container, {
			value: this.query, language: lang,
			theme, automaticLayout: true,
			minimap: { enabled: false }, scrollBeyondLastLine: false,
			fontSize
		});
    }

    /** @param { HTMLElement } obj */
    showItemsGroup(obj){

        const groupName = obj.dataset.groupName;
        const items = document.getElementsByClassName(`${groupName}-item`);
        
        if(items.length > 0){
            
            const showClassGroup = `${groupName}-item-show`;
            const addOrRemove = items[0].classList.contains(showClassGroup) ? 'remove' : 'add';
            obj.getElementsByClassName('drop-down-icon')[0].firstChild.classList[addOrRemove]('rotate-menu-arrow');
            for(const item of items){
                item.classList[addOrRemove](showClassGroup);
            }
        }
    }

    handleNodeMinimize(icon, minimizedWidth, nodeType){
        const mainContainer = icon.parentNode;
        const outerContainer = icon.parentNode.parentNode.parentNode;
		if(mainContainer.classList.contains('minimize-node')){
            const normalWidth = mainContainer.normalWidth;
            outerContainer.style.width = normalWidth;
			mainContainer.classList.remove('minimize-node');
            icon.innerHTML = '_';
            icon.style.height = '27px';
            if(nodeType === 'transform')
                mainContainer.querySelector('.statusicon').style.width = '20px';

        }else{
            mainContainer.normalWidth = outerContainer.style.width;
			mainContainer.classList.add('minimize-node');
            outerContainer.style.width = minimizedWidth;
            icon.innerHTML = '+';
            icon.style.height = '15px';

            if(nodeType === 'transform')
                mainContainer.querySelector('.statusicon').style.width = '56px';
        }
    }

}