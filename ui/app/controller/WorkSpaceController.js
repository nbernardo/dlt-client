import { BaseService } from "../../@still/component/super/service/BaseService.js";

export class WorkSpaceController extends BaseService {


    registerEvents() {

        let transform = '';
        window.drawshowpopup = function showpopup(e) {
            e.target.closest(".drawflow-node").style.zIndex = "9999";
            e.target.children[0].style.display = "block";
            //document.getElementById("modalfix").style.display = "block";

            //e.target.children[0].style.transform = 'translate('+translate.x+'px, '+translate.y+'px)';
            transform = draweditor.precanvas.style.transform;
            draweditor.precanvas.style.transform = '';
            draweditor.precanvas.style.left = draweditor.canvas_x + 'px';
            draweditor.precanvas.style.top = draweditor.canvas_y + 'px';
            console.log(transform);

            //e.target.children[0].style.top  =  -draweditor.canvas_y - draweditor.container.offsetTop +'px';
            //e.target.children[0].style.left  =  -draweditor.canvas_x  - draweditor.container.offsetLeft +'px';
            draweditor.editor_mode = "fixed";

        }

        window.drawclosemodal = function closemodal(e) {
            e.target.closest(".drawflow-node").style.zIndex = "2";
            e.target.parentElement.parentElement.style.display = "none";
            //document.getElementById("modalfix").style.display = "none";
            draweditor.precanvas.style.transform = transform;
            draweditor.precanvas.style.left = '0px';
            draweditor.precanvas.style.top = '0px';
            draweditor.editor_mode = "edit";
        }

        window.drawchangeModule = function changeModule(event) {
            var all = document.querySelectorAll(".menu ul li");
            for (var i = 0; i < all.length; i++) {
                all[i].classList.remove('selected');
            }
            event.target.classList.add('selected');
        }

        window.drawchangeMode = function changeMode(option) {

            //console.log(lock.id);
            if (option == 'lock') {
                lock.style.display = 'none';
                unlock.style.display = 'block';
            } else {
                lock.style.display = 'block';
                unlock.style.display = 'none';
            }

        }


        let mobile_item_selec = '';
        let mobile_last_move = null;

        window.drawpositionMobile = function positionMobile(ev) {
            mobile_last_move = ev;
        }

        window.drawallowDrop = function allowDrop(ev) {
            ev.preventDefault();
        }

        window.drawdrag = function drag(ev) {
            if (ev.type === "touchstart") {
                mobile_item_selec = ev.target.closest(".drag-drawflow").getAttribute('data-node');
            } else {
                ev.dataTransfer.setData("node", ev.target.getAttribute('data-node'));
            }
        }

        window.drawdrop = function drop(ev) {
            if (ev.type === "touchend") {
                var parentdrawflow = document.elementFromPoint(mobile_last_move.touches[0].clientX, mobile_last_move.touches[0].clientY).closest("#drawflow");
                if (parentdrawflow != null) {
                    addNodeToDrawFlow(mobile_item_selec, mobile_last_move.touches[0].clientX, mobile_last_move.touches[0].clientY);
                }
                mobile_item_selec = '';
            } else {
                ev.preventDefault();
                var data = ev.dataTransfer.getData("node");
                addNodeToDrawFlow(data, ev.clientX, ev.clientY);
            }

        }

        function addNodeToDrawFlow(name, pos_x, pos_y) {
            if (draweditor.editor_mode === 'fixed') {
                return false;
            }
            pos_x = pos_x * (draweditor.precanvas.clientWidth / (draweditor.precanvas.clientWidth * draweditor.zoom)) - (draweditor.precanvas.getBoundingClientRect().x * (draweditor.precanvas.clientWidth / (draweditor.precanvas.clientWidth * draweditor.zoom)));
            pos_y = pos_y * (draweditor.precanvas.clientHeight / (draweditor.precanvas.clientHeight * draweditor.zoom)) - (draweditor.precanvas.getBoundingClientRect().y * (draweditor.precanvas.clientHeight / (draweditor.precanvas.clientHeight * draweditor.zoom)));

            const dest = 1, source = 1;

            switch (name) {
                case 'bucket':
                    var bucket = `
                    <div>
                        <div class="title-box"><i class="fab fa-bitbucket"></i><span> Data Source - Bucket</span></div>
                    </div>
                    `;
                    draweditor.addNode('bucket', !source, dest, pos_x, pos_y, 'bucket', {}, bucket);
                    break;
                case 'slack':
                    var slackchat = `
                    <div>
                        <div class="title-box"><i class="fab fa-slack"></i> Slack chat message</div>
                    </div>
                    `;
                    draweditor.addNode('slack', source, !dest, pos_x, pos_y, 'slack', {}, slackchat);
                    break;
                case 'github':
                    var githubtemplate = `
                    <div>
                        <div class="title-box"><i class="fab fa-github "></i> Github Stars</div>
                        <div class="box">
                        <p>Enter repository url</p>
                        <input type="text" df-name>
                        </div>
                    </div>
                    `;
                    draweditor.addNode('github', !source, dest, pos_x, pos_y, 'github', { "name": '' }, githubtemplate);
                    break;
                case 'telegram':
                    var telegrambot = `
                    <div>
                        <div class="title-box"><i class="fab fa-telegram-plane"></i> Telegram bot</div>
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
                    draweditor.addNode('telegram', source, !dest, pos_x, pos_y, 'telegram', { "channel": 'channel_3' }, telegrambot);
                    break;
                case 'aws':
                    var aws = `
                    <div>
                        <div class="title-box"><i class="fab fa-aws"></i> Aws Save </div>
                        <div class="box">
                        <p>Save in aws</p>
                        <input type="text" df-db-dbname placeholder="DB name"><br><br>
                        <input type="text" df-db-key placeholder="DB key">
                        <p>Output Log</p>
                        </div>
                    </div>
                    `;
                    draweditor.addNode('aws', source, source, pos_x, pos_y, 'aws', { "db": { "dbname": '', "key": '' } }, aws);
                    break;
                case 'log':
                    var log = `
                    <div>
                        <div class="title-box"><i class="fas fa-file-signature"></i> Save log file </div>
                    </div>
                    `;
                    draweditor.addNode('log', 1, 0, pos_x, pos_y, 'log', {}, log);
                    break;
                case 'google':
                    var google = `
                    <div>
                        <div class="title-box"><i class="fab fa-google-drive"></i> Google Drive save </div>
                    </div>
                    `;
                    draweditor.addNode('google', 1, 0, pos_x, pos_y, 'google', {}, google);
                    break;
                case 'email':
                    var email = `
                    <div>
                        <div class="title-box"><i class="fas fa-at"></i> Send Email </div>
                    </div>
                    `;
                    draweditor.addNode('email', 1, 0, pos_x, pos_y, 'email', {}, email);
                    break;

                case 'template':
                    var template = `
                    <div>
                        <div class="title-box"><i class="fas fa-code"></i> Template</div>
                        <div class="box">
                            Ger Vars
                            <textarea df-template></textarea>
                            Output template with vars
                        </div>
                    </div>
                    `;
                    draweditor.addNode('template', 1, 1, pos_x, pos_y, 'template', { "template": 'Write your template' }, template);
                    break;
                case 'multiple':
                    var multiple = `
                    <div>
                        <div class="box">
                            Multiple!
                        </div>
                    </div>
                    `;
                    draweditor.addNode('multiple', 3, 4, pos_x, pos_y, 'multiple', {}, multiple);
                    break;
                case 'personalized':
                    var personalized = `
                    <div>
                    Personalized
                    </div>
                    `;
                    draweditor.addNode('personalized', 1, 1, pos_x, pos_y, 'personalized', {}, personalized);
                    break;
                case 'dbclick':
                    var dbclick = `
                    <div>
                        <div class="title-box"><i class="fas fa-mouse"></i> Db Click</div>
                        <div class="box dbclickbox" ondblclick="drawshowpopup(event)">
                            Db Click here
                            <div class="modal" style="display:none">
                            <div class="modal-content">
                                <span class="close" onclick="drawclosemodal(event)">&times;</span>
                                Change your variable {name} !
                                <input type="text" df-name>
                            </div>

                            </div>
                        </div>
                    </div>
                    `;
                    draweditor.addNode('dbclick', 1, 1, pos_x, pos_y, 'dbclick', { name: '' }, dbclick);
                    break;

                default:
            }
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
        editor.on('nodeCreated', function (id) {
            console.log("Node created " + id);
        })

        editor.on('nodeRemoved', function (id) {
            console.log("Node removed " + id);
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
            console.log('Position mouse x:' + position.x + ' y:' + position.y);
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

}