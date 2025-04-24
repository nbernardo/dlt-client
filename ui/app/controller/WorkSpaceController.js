import { BaseController } from "../../@still/component/super/service/BaseController.js";
import { Components } from "../../@still/setup/components.js";
import { Bucket } from "../components/node-types/Bucket.js";
import { CleanerType } from "../components/node-types/CleanerType.js";
import { DuckDBOutput } from "../components/node-types/DuckDBOutput.js";

class NodeType {
    tmplt; data;
}

export class WorkSpaceController extends BaseController {

    editor;
    transform = '';
    edgeTypeAdded = {};

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
            const tmpl = this.edgeType(inType);
            if (tmpl != null)
                this.editor.addNode(name, source, dest, pos_x, pos_y, name, {}, tmpl);
            return;
        }

        if (['Bucket', 'DuckDBOutput'].includes(inType)) {
            const cmpTypes = {
                'Bucket': Bucket,
                'DuckDBOutput': DuckDBOutput
            }
            const nodeId = this.getNodeId();
            const { template: tmpl, component } = await Components.new(cmpTypes[inType], nodeId);
            const { inConnectors, outConnectors } = component;
            this.editor.addNode(name, inConnectors, outConnectors, pos_x, pos_y, name, {}, tmpl);
            this.clearHTML(nodeId);
            return;
        }


        if (!['Bucket', 'personalized', 'dbclick'].includes(name)) {
            const type = this[`${name}Type`]({ name, label, icon, img })
            this.editor.addNode(name, source, dest, pos_x, pos_y, name, type.data, type.tmplt);
        }

        if (name == 'personalized') {
            var personalized = `
                <div>
                Personalized
                </div>
                `;
            this.editor.addNode('personalized', 1, 1, pos_x, pos_y, 'personalized', {}, personalized);
        }

        if (name == 'dbclick') {
            const { template: tmpl } = await Components.new(CleanerType)
            this.editor.addNode('dbclick', 1, 1, pos_x, pos_y, 'dbclick', { name: '' }, tmpl);
        }
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
        var all = document.querySelectorAll(".menu ul li");
        for (var i = 0; i < all.length; i++) {
            all[i].classList.remove('selected');
        }
        event.target.classList.add('selected');
    }

    /** @returns { NodeType } */
    genericType({ name, label, icon, img }) {
        const tmplt = `
            <div>
                <div class="title-box"><i class="${icon}"></i><span> ${label}</span></div>
            </div>
        `;
        return { tmplt, data: {} }
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

    /** @returns { NodeType } */
    githubType({ name, label, icon, img }) {
        const tmplt = `
            <div>
                <div class="title-box"><i class="fab fa-github "></i> Github Stars</div>
                <div class="box">
                <p>Enter repository url</p>
                <input type="text" df-name>
                </div>
            </div>
        `;
        return { tmplt, data: { "name": '' } }
    }

    /** @returns { NodeType } */
    telegramType({ name, label, icon, img }) {
        const tmplt = `
        <div>
            <div class="title-box"><img src="${img}" style="width: 25px;"> ${label}</div>
            <div class="box">
            <p>Send to telegram</p>
            <p>select channel</p>
            <select df-channel>
                <option value="channel_1">Channel 1</option>
                <option value="channel_2">Channel 2</option>
                <option value="channel_3">Channel 3</option>
                <option value="channel_4">Channel 4</option>
            </select>
            </div>
        </div>
        `;
        return { tmplt, data: { "channel": 'channel_3' } }
    }


    /** @returns { NodeType } */
    awsType({ name, label, icon, img }) {
        const tmplt = `
        <div>
            <div class="title-box"><i class="${icon}"></i> Aws Save </div>
            <div class="box">
            <p>${label}</p>
            <input type="text" df-db-dbname placeholder="DB name"><br><br>
            <input type="text" df-db-key placeholder="DB key">
            <p>Output Log</p>
            </div>
        </div>
        `;
        return {
            tmplt, data: { "db": { "dbname": '', "key": '' } }
        }
    }

    events() {
        return {
            drag: window.drawdrag,
            drop: window.drawdrop,
            positionMobile: window.drawpositionMobile
        }
    }

    handleListeners(editor) {

        // Events!
        const obj = this;
        editor.on('nodeCreated', function (id) {
            console.log("Node created " + id);
        })

        editor.on('nodeRemoved', function (id) {
            if (id == obj.edgeTypeAdded['Start'])
                delete obj.edgeTypeAdded['Start'];

            if (id == obj.edgeTypeAdded['End'])
                delete obj.edgeTypeAdded['End'];
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

        editor.on('connectionCreated', function (connection) {
            console.log('Connection created');
            console.log(connection);
        })

        editor.on('connectionRemoved', function (connection) {
            console.log('Connection removed');
            console.log(connection);
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
        const allNodes = Object.keys(this.editor.drawflow.drawflow.Home.data);
        if (allNodes.length == 0) return 1;
        return Number(allNodes.slice(-1)[0]) + 1;
    }

    static getNode(nodeId) {
        const obj = WorkSpaceController.get();
        return obj.editor.drawflow.drawflow.Home.data[nodeId];
    }

    clearHTML(nodeId) {
        WorkSpaceController.getNode(nodeId).html = '';
    }


    export() {
        const exportResult = this.editor.export();
        console.log(JSON.stringify(exportResult, null, 4));
    }

}