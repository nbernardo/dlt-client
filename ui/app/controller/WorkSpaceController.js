import { BaseController } from "../../@still/component/super/service/BaseController.js";
import { Components } from "../../@still/setup/components.js";
import { StillAppSetup } from "../../config/app-setup.js";
import { AppTemplate } from "../../config/app-template.js";
import { NodeTypeInterface } from "../components/node-types/mixin/NodeTypeInterface.js";

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

    resetEdges() {
        this.edgeTypeAdded = {};
        this.formReferences.clear();
        this.validationErrors = [];
        //this.idCounter = 0;
        this.cmpIdToNodeIdMap = {};
        this.pplineStatus = {};
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

    drag(ev) {
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
            const [pos_x, pos_y] = [ev.clientX, ev.clientY];
            const data = { name, label, icon, img, pos_x, pos_y, source, dest };

            await this.addNodeToDrawFlow(data);
        }
    }

    async addNodeToDrawFlow({ name, label, icon, img, pos_x, pos_y, source, dest }) {
        const inType = name;
        if (this.editor.editor_mode === 'fixed') return false;
        if (['bucket', 'slack', 'log'].includes(name)) name = 'generic';

        pos_x = pos_x * (this.editor.precanvas.clientWidth / (this.editor.precanvas.clientWidth * this.editor.zoom)) - (this.editor.precanvas.getBoundingClientRect().x * (this.editor.precanvas.clientWidth / (this.editor.precanvas.clientWidth * this.editor.zoom)));
        pos_y = pos_y * (this.editor.precanvas.clientHeight / (this.editor.precanvas.clientHeight * this.editor.zoom)) - (this.editor.precanvas.getBoundingClientRect().y * (this.editor.precanvas.clientHeight / (this.editor.precanvas.clientHeight * this.editor.zoom)));

        if (['Start', 'End'].includes(inType)) {
            return this.addStartOrEndNode(name, source, dest, pos_x, pos_y);
        }

        const nodeId = this.getNodeId();
        const { template: tmpl, component } = await Components.new(inType, { nodeId });
        this.handleAddNode(component, nodeId, name, pos_x, pos_y, tmpl);

    }

    async processImportingNodes(nodeData) {

        const inOutputMapping = { };

        const nodeList = Object.entries(nodeData);
        let nodeId = 0;

        for(let [_, { class: name, data, pos_x, pos_y, source, dest, inputs, outputs }] of nodeList){
            if (!['Start', 'End'].includes(name)) {

                //The extracted fields and nodeId are the fields inside the components itself ( from node-types folder )
                let { componentId: removedId, ...fields } = data;

                const { template: tmpl, component } = await Components.new(name, { nodeId: ++nodeId, ...fields, isImport: true });

                this.handleAddNode(component, nodeId, name, pos_x, pos_y, tmpl);
                setTimeout(() => Object.keys(fields).forEach((f) => component[f] = fields[f]), 10);

                //We first collects all links to render after all nodes are in the diagram
                inOutputMapping[nodeId] = { inputs, outputs };

            }else{
                
                [source, dest] = [ name === 'End', name === 'Start' ];
                this.addStartOrEndNode(name, source, dest, pos_x, pos_y);
                inOutputMapping[++nodeId] = { inputs, outputs };
            }            
        }

        Object.keys(inOutputMapping).forEach(nodeId => {

            const { inputs, outputs } = inOutputMapping[nodeId];
            outputs?.output_1?.connections.forEach((link) => {
                this.editor.addConnection(Number(nodeId), Number(link.node), 'output_1', 'input_1');
            });

        });

    }

    addStartOrEndNode(name, source, dest, pos_x, pos_y){
        const tmpl = this.edgeType(name);
        if (tmpl != null)
            return this.editor.addNode(name, source, dest, pos_x, pos_y, name, {}, tmpl);
        return;
    }

    handleAddNode(component, nodeId, name, pos_x, pos_y, tmpl){

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
            <div class="edge-label">
                ${type}
            </div>
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

    handleListeners(editor) {

        // Events!
        const obj = this;
        editor.on('nodeCreated', function (id) {
            console.log("Node created " + id);
        })

        editor.on('nodeRemoved', function (id) {
            if (id == obj.edgeTypeAdded[NodeTypeEnum.START])
                return delete obj.edgeTypeAdded[NodeTypeEnum.START];

            if (id == obj.edgeTypeAdded[NodeTypeEnum.END])
                return delete obj.edgeTypeAdded[NodeTypeEnum.END];

            //Remove the form so it does not gets considered 
            //when validating the pipeline submittions/save
            obj.formReferences.delete(Number(id));
            delete obj.edgeTypeAdded[id];
        })

        editor.on('nodeSelected', function (id) {
            console.log("Node selected " + id);
        })

        editor.on('moduleCreated', function (name) {
            console.log("Module Created " + name);
        })

        editor.on('moduleChanged', function (name) {
            console.log("Module Changed " + name);
        })

        editor.on('connectionCreated', connections => obj.onConnectionCreate(connections, obj));

        editor.on('connectionRemoved', function (connection) {
            const { output_id, input_id } = connection;
            if (output_id != obj.edgeTypeAdded[NodeTypeEnum.START])
                obj.edgeTypeAdded[output_id].delete(input_id);

            if (input_id != obj.edgeTypeAdded[NodeTypeEnum.END])
                obj.edgeTypeAdded[input_id].delete(output_id);
        })

        editor.on('mouseMove', function (position) {
            //console.log('Position mouse x:' + position.x + ' y:' + position.y);
        })

        editor.on('nodeMoved', function (id) {
            console.log("Node moved " + id);
        })

        editor.on('zoom', function (zoom) {
            console.log('Zoom level ' + zoom);
        })

        editor.on('translate', function (position) {
            console.log('Translate x:' + position.x + ' y:' + position.y);
        })

        editor.on('addReroute', function (id) {
            console.log("Reroute added " + id);
        })

        editor.on('removeReroute', function (id) {
            console.log("Reroute removed " + id);
        })

    }

    getNodeId() {
        //const allNodes = Object.keys(this.editor.drawflow.drawflow.Home.data);
        //if (allNodes.length == 0) return 1;
        //return Number(allNodes.slice(-1)[0]) + 1;
        return ++this.idCounter;
    }

    static getNode(nodeId) {
        const obj = WorkSpaceController.get();
        return obj.editor.drawflow.drawflow.Home.data[nodeId] || {};
    }

    clearHTML(nodeId) {
        WorkSpaceController.getNode(nodeId).html = '';
    }


    export() {
        const exportResult = this.editor.export();
        console.log(JSON.stringify(exportResult, null, 4));
    }

    socketChannelSetup(io, socketData) {

        const wssAddr = StillAppSetup.config.get('websockerAddr');
        const socket = io(wssAddr, { transports: ["websocket"] });

        socket.on('connect', () => { });
        socket.on('connected', (data) => socketData.sid = data.sid);

        socket.on('pplineError', ({ componentId, sid, error }) => {
            WorkSpaceController.addFailedStatus(componentId);
            AppTemplate.toast.error(error.message);
        });

        socket.on('pplineStepStart', ({ componentId, sid }) => {
            if (!this.pplineSteps[sid]) this.pplineSteps[sid] = new Set();
            this.pplineSteps[sid].add(componentId);
            WorkSpaceController.addRunningStatus(componentId);
        });

        socket.on('pplineStepSuccess', ({ componentId, sid }) => {
            const nodeId = this.cmpIdToNodeIdMap[componentId];
            const node = WorkSpaceController.getNode(nodeId);
            if (Object.keys(node.outputs).length > 0)
                WorkSpaceController.addPreSuccessStatus(componentId);
        });

        socket.on('pplineSuccess', ({ sid }) => {
            const tasks = this.pplineSteps[sid];
            this.pplineStatus = PPLineStatEnum.Finished;
            [...tasks].forEach(WorkSpaceController.addSuccessStatus);
        });

        socket.on('pplineTrace', ({ data: trace }) => {
            console.log('FROM BACK: ',trace);
        });

    }

    static addRunningStatus(containerHTMLClass) {
        document
            .querySelector(`.${containerHTMLClass}`)
            .querySelector('div[class=title-box]')
            .querySelector('.statusicon')
            .className = 'statusicon running-status';
    }

    static remRunningStatus(containerHTMLClass) {
        document
            .querySelector(`.${containerHTMLClass}`)
            .querySelector('div[class=title-box]')
            .querySelector('.statusicon')
            .classList.remove('running-status');
    }

    static addFailedStatus(containerHTMLClass) {
        document
            .querySelector(`.${containerHTMLClass}`)
            .querySelector('div[class=title-box]')
            .querySelector('.statusicon')
            .className = 'statusicon failed-status';
    }

    static addSuccessStatus(containerHTMLClass) {
        document
            .querySelector(`.${containerHTMLClass}`)
            .querySelector('div[class=title-box]')
            .querySelector('.statusicon')
            .className = 'statusicon success-status';
    }

    static addPreSuccessStatus(containerHTMLClass) {
        document
            .querySelector(`.${containerHTMLClass}`)
            .querySelector('div[class=title-box]')
            .querySelector('.statusicon')
            .className = 'statusicon pre-success-status';
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

        const { data: dataIn } = WorkSpaceController.getNode(input_id);
        const { data: dataOut } = WorkSpaceController.getNode(output_id);

        const destCmpId = dataIn.componentId, srcCmpId = dataOut.componentId;

        const /** @type { NodeTypeInterface } */ destCmp = Components.ref(destCmpId) || {};
        const /** @type { NodeTypeInterface } */ srcCmp = Components.ref(srcCmpId) || {};

        if('onOutputConnection' in srcCmp){
            if('onInputConnection' in destCmp){
                (async () => {
                    const sourceData = await srcCmp.onOutputConnection();
                    await destCmp.onInputConnection({ data: sourceData, type: srcCmp.getName() });
                })();
            }
        }

        if (dataIn?.componentId) {
            const inputs = obj.getInputsByNodeId(dataIn?.componentId);
            [...inputs].forEach(el => {
                el.style.background = 'white';
                el.classList.remove('blink');
            });
        }

        if (output_id != obj.edgeTypeAdded[NodeTypeEnum.START])
            obj.edgeTypeAdded[output_id].add(input_id);

        if (input_id != obj.edgeTypeAdded[NodeTypeEnum.END])
            obj.edgeTypeAdded[input_id].add(output_id);
    }

    copyToClipboard(content) {
        navigator.clipboard.writeText(content);
    }

    disableNodeFormInputs(formWrapClass){
        const disable = true, defautlFields = 'input[type=text], select';
        // Disable the default form inputs
        document.querySelector('.'+formWrapClass).querySelectorAll(defautlFields).forEach(elm => {
            elm.disabled = disable;
        });
    }

}