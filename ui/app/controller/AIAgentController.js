import { AIAgent } from "../components/agent/AIAgent.js";
import { AIResponseLinterUtil } from "../components/agent/AIResponseLinterUtil.js";
import { agentOptions, aiStartOptions, botSubRoutineCall, dontFollowAgentFlow, unkwonRequest } from "../components/agent/chatbotbrain/main.js";
import { Workspace } from "../components/workspace/Workspace.js";
import { WorkspaceService } from "../services/WorkspaceService.js";
import { WorkSpaceController } from "./WorkSpaceController.js";

export class AIAgentController {

    static agentActiveFlow = null;

    /** @type { AIAgent } */
    agentInstance = null;

    initAgentActiveFlow(flowName){
        AIAgentController.agentActiveFlow = flowName;
        return this.initAgentFLowMessage(flowName);
    }

    initAgentFLowMessage(flowName){
        const message = 'Great, what do you want about'
        const flowMessage = {
            'pipeline': message+ ' Pipeline?',
            'data-query': message+ ' Data query?'
        };
        return flowMessage[flowName];
    }

    setAgentFlow = (flowName) => AIAgentController.agentActiveFlow = flowName;

    /** @returns { null|'pipeline'|'data-query' } */
    getActiveFlow = () => AIAgentController.agentActiveFlow;
    
    /** 
     * @param {string} fields
     * @param { Array<Array> } data
     * @param { Workspace } workspaceComponent 
      */
    parseDataToTable(fields, data, workspaceComponent, actualQuery, database) {

        const fieldsHeader =
            '<tr>' + fields.split(',')
                .map(field => `<th class="right">${field.trim()}</th>`).join('').replaceAll('\'', '')
            + '</tr>';

        const tableBody = data.map(row =>
            `<tr>${row.map(fieldVal => `<td class="right">${fieldVal}</td>`).join('')}</tr>`
        ).join('');

        const scrolingLbl = `<b style="display: block;">Scoll to right if more content is hidden</b>`;
        const resultId = ++workspaceComponent.controller.aiAgentExpandViewCount;

        // Workspace component will parse the event, then inner.expandDataTableView() points to
        // a method that is implemented within itself, hence inner
        let expandViewLink = `<a href="#" onclick="inner.expandDataTableView(${resultId})">Expand View</a>`;
        expandViewLink = workspaceComponent.parseEvents(expandViewLink);

        const actualTable = `${scrolingLbl}<br>${expandViewLink}
                <table class="chatbot-response-table">
                    <thead>${fieldsHeader}</thead>
                    <tbody>${tableBody}</tbody>
                </table>`;

        // Passing data to the controller so it can be accessed by the Workspace component
        setTimeout(() => {
            workspaceComponent.controller.aiAgentExpandView.data = data;
            workspaceComponent.controller.aiAgentExpandView.fields = fields;
            workspaceComponent.controller.aiAgentExpandView.database = database;
            workspaceComponent.controller.aiAgentExpandView.query = AIResponseLinterUtil.formatSQL(actualQuery),
            workspaceComponent.controller.aiAgentExpandView.initialTable = actualTable;
            workspaceComponent.controller.addAIAgentGridExpand(resultId);
        },0);

        return actualTable;

    }

    async parsePipelineCreationContent(content){
        WorkSpaceController.instance().wSpaceComponent.resetWorkspace();
        content = JSON.parse(content);      
        for(const node of Object.values(content)){
            const { nodeName, data } = node;
            await WorkSpaceController.instance().createNode(nodeName, (data || {}));
        }
        await WorkSpaceController.instance().linkAgentCreatedNodes();
    }

    /** @param {String} botResponse */
    setAgentRoute(botResponse){

        if (botResponse.includes(agentOptions.pipeline) && this.getActiveFlow() != 'pipeline')
            this.setAgentFlow('pipeline');
            
        if(botResponse.includes(agentOptions.dataQuery) && this.getActiveFlow() != 'data-query')
            this.setAgentFlow('data-query');
        
        // Cleans up the AI Agent flow type if present
		return botResponse
            .replace(agentOptions.pipeline, '')
            .replace(agentOptions.dataQuery, '')
            .replace(dontFollowAgentFlow, '')
            .replace(botSubRoutineCall, '')
    }

    dataToTable(data, title = null){

        const headerStyle = 'style="background: #808080b5; color: white; font-weight: bold;"';
        let rows = Object.values(Object.values(data)), tableBody = '';
        const fields = Object.keys(rows[0]);
        const header = `${fields.map(field => `<td>${field}</td>`).join('')}`;
        title = title != null ? `<tr ${headerStyle}><td colspan="${fields.length}">${title}</td></tr>` : '';

        for(const row of rows)
            tableBody += `<tr>${fields.map(field => `<td>${row[field]}</td>`).join('')}</tr>`;
        
        return `<table>
                    <thead>
                        ${title}
                        <tr ${headerStyle}>${header}</tr>
                    </thead>
                    <tbody>${tableBody}</tbody>
                </table>
                `;

    }

    initialAiAgentOptions(complementMessage, agentInstance){
        let content = `
                <div class='start-agent-orientation-msg' style='margin-top: -28px'>${complementMessage}</div>
                <div class="ai-agent-options" style='margin-top: -23px'>
                    <div class='ai-flow-option' onclick="inner.setAgentFlow('pipeline')">
                        <img class='icon-image' src='app/assets/imgs/ai/pipeline-icon.svg'>
                        <div class="agent-flow-icon">1. Pipeline</div>
                    </div>
                    <div class='ai-flow-option' onclick="inner.setAgentFlow('data-query')">
                        <img class='icon-image' src='app/assets/imgs/ai/dbquery-icon.svg'>
                        <div class="agent-flow-icon">2. Data query</div>
                    </div>
                </div>
            `;
        return agentInstance.parseEvents(content);
    }

    /** @param {AIAgent} agentInstance */
    displayInitialChatOptions(agentInstance){        
        if(agentInstance.startedInstance === null){
            const initialMessage = 'Hey, I\'m more than happy to help you.<br><br>'
            +'<div style="margin-top: -7px;">Which of the bellow categories to you want to talk about?</div>';
            
            const content = this.initialAiAgentOptions(initialMessage, agentInstance);
            agentInstance.createMessageBubble(`${content}`, 'agent', 'DLT Workspace');
        }
    }

    /** @param {AIAgent} agentInstance */
    setupBotSubRoutine(agentInstance){

        const self = agentInstance;
        self.botInstance.setSubroutine('setDataQueryFlow', () => this.setAgentFlow('data-query'));
        self.botInstance.setSubroutine('setPipelineFlow', () => this.setAgentFlow('pipeline'));
        self.botInstance.setSubroutine('setDontFollowAgent', (_, args) => self.dontFollowAgentFlag = args[0]);

        self.botInstance.setSubroutine('showSerets', (_, args) => {
            (async () => {										
                let foundSecrets = await WorkspaceService.listSecrets(args[1] || 'all'), secrets = '';

                if(Object.keys((foundSecrets['db'] || {})).length > 0)
                    secrets += this.dataToTable(foundSecrets['db'], 'Database Secrets');

                if(Object.keys((foundSecrets['api'] || {})).length > 0)
                    secrets += this.dataToTable(foundSecrets['api'], 'API Secrets');

                if(Array.isArray(foundSecrets) && args[1])
                    secrets += this.dataToTable(foundSecrets, `${args[1] == 1 ? 'Database' : 'API'} Secrets`);

                self.setAgentLastMessage(args[0], secrets, true);
            })();
        });
        
        self.botInstance.setSubroutine('displayIAAgentOptions', () => {
            const complementMessage = `${unkwonRequest}<br>${aiStartOptions}`
            const content = this.initialAiAgentOptions(complementMessage, this.agentInstance);
            self.createMessageBubble(null,'agent','DLT Workspace');
            self.setAgentLastMessage(null, content, true);
        });
    }

	shrinkChatSize(){
		if(document.querySelector('.ai-agent-placeholder').classList.contains('ai-agent-placeholder-shrinked')){
			this.unshrinkChatSize();
		}else{
			document.querySelector('.ai-agent-placeholder').classList.add('ai-agent-placeholder-shrinked');
			document.querySelector('.ai-agent-sent-messages-count-placehoder').style.flexDirection = 'column';
		}
	}

	unshrinkChatSize(){
		document.querySelector('.ai-agent-placeholder').classList.remove('ai-agent-placeholder-shrinked');
		document.querySelector('.ai-agent-sent-messages-count-placehoder').style.flexDirection = 'row';
	}

	setUserPrompt(content){
		document.getElementById('ai-chat-user-input').value = content;
		document.getElementById('ai-chat-user-input').focus();
	}

	loadingContent() {
		return `
			<div class="mini-loader-container">
				<div class="mini-loader-dot" style="background: black;"></div>
				<div class="mini-loader-dot" style="background: black;"></div>
				<div class="mini-loader-dot" style="background: black;"></div>
			</div>
		`;
	}

}