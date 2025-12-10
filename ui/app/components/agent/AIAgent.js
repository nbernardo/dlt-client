import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { Assets } from "../../../@still/util/componentUtil.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";
import { AIAgentController } from "../../controller/AIAgentController.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";
import { markdownToHtml } from "../../util/Markdown.js";
import { Workspace } from "../workspace/Workspace.js";
import { content, unkwonRequest } from "./chatbotbrain/main.js";

export class AIAgent extends ViewComponent {

	isPublic = true;

	/** @type { Workspace } */
	$parent;

	/** @Prop */ outputContainer;
	/** @Prop */ outputContainerId = '_' + UUIDUtil.newId();
	/** @Prop */ resizeHandle;
	/** @Prop */ isDragging = false;
    /** @Prop */ startY = 0;
    /** @Prop */ startHeight = 0;
    /** @Prop */ appContainer;
    /** @Prop */ isThereAgentMessage = false;
    /** @Prop */ startedInstance = null;
    /** @Prop */ showLimitReachedWarn = null;
    /** @Prop */ botInstance = null;

	/** @type { HTMLParagraphElement } */
	static lastAgentParagraph;

	/** @Prop */
	unloadNamespaceMsg = 'Could not load details about your namespace.';

	// Because this controller is not shared with other component hence
	// we're instantiating straight instead of @Inject, this Will also
	// make the component loading faster, @Prop is required in this scenarios 
	/** @Prop @type { AIAgentController }*/
	controller = new AIAgentController();

	sentMessagesCount = 0;

	/** @Prop */ maxAllowedMessages = -1;

	async stBeforeInit() {
		await Assets.import({ path: '/app/assets/css/agent.css' });
		await Assets.import({ path: 'https://unpkg.com/rivescript@latest/dist/rivescript.min.js', type:'js' });

		setTimeout(async () => {
			this.botInstance = new RiveScript();

			this.botInstance.setSubroutine('setDataQueryFlow', () => this.setAgentFlow('data-query'));
			this.botInstance.setSubroutine('setPipelineFlow', () => this.setAgentFlow('pipeline'));

			await this.botInstance.stream(content);
			await this.botInstance.sortReplies();
		},0);
	}

	stOnRender({totalMessages, messageCountLimit}){		
		if(totalMessages) this.sentMessagesCount = Number(totalMessages);
		if(messageCountLimit) this.maxAllowedMessages = Number(messageCountLimit);

		/** this.sentMessagesCount will behave as a prop since it didn't render yet */
		if(this.maxAllowedMessages != -1 &&  this.sentMessagesCount >= this.maxAllowedMessages)
			this.showLimitReachedWarn = true;
	}

	async stAfterInit() {

		this.appContainer = document.querySelector('.ai-app-container');
		this.outputContainer = document.getElementById(this.outputContainerId);
		this.resizeHandle = document.querySelector('.ai-agent-height-resize');
		this.resizeHandle.style.backgroundColor = '#047857'
		this.setResizeHandling();
		await this.startNewAgent();

		this.sentMessagesCount.onChange((val) => {
			if(this.maxAllowedMessages != -1 && val >= this.maxAllowedMessages)
				this.showLimitReachedWarn = true;
		})

		this.$parent.service.aiAgentNamespaceDetails;

	}

	async startNewAgent(retry = false) {
		try {
			this.startedInstance = await WorkspaceService.startChatConversation();
			
			if(this.startedInstance.start === false && retry === false){
				this.createMessageBubble(`<div class="agent-no-start-error">${this.startedInstance.error}</div>`, 'agent', 'DLT Workspace');
				this.startedInstance = null;
			}else{
				const initialMessage = 'Hey, I\'m more than happy to help you.<br><br>'
									   +'<div style="margin-top: -7px;">Which of the bellow categories to you want to talk about?</div>';
				let content = `
					<div class='start-agent-orientation-msg' style='margin-top: -28px'>${initialMessage}</div>
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
				content = this.parseEvents(content);
				this.createMessageBubble(`${content}`, 'agent', 'DLT Workspace');
			}
		} catch (error) { }
	}

	setAgentFlow = (flowName) => {
		this.createMessageBubble(flowName, 'user');
		const initMessage = this.controller.initAgentActiveFlow(flowName);
		this.createMessageBubble(`${initMessage}`, 'agent', 'DLT Workspace');
	};

	async sendChatRequest(event) {
		if (event.key === 'Enter') {

			event.preventDefault();
			const message = event.target.value;

			const botResponse = await this.botMessage(event);
			const isFlowNotSet = this.controller.getActiveFlow() == null;
			if(botResponse.startsWith(unkwonRequest) && isFlowNotSet){
				event.target.value = '';
				return this.createMessageBubble(botResponse, 'agent', 'DLT Workspace');
			}

			let dataTable = null;
			//this.createMessageBubble(message, 'user');
			this.scrollToBottom();
						
			if(this.startedInstance === null){
				this.startNewAgent(true); /** This will retry to connect with the Agent Backend */

				/** Because retry took place once, if it successfull connects, then the startedInstance won't be null */
				if(this.startedInstance === null){
					const result = "No agent was initiated since you don't/didn't have data in the namespace. I can update myself if you ask.";
					return this.createMessageBubble(`<div class="agent-no-start-msg">${result}</div>`, 'agent', 'DLT Workspace');
				}
			}

			this.createMessageBubble(this.loadingContent(), 'agent');
			this.sentMessagesCount = this.sentMessagesCount.value + 1;
			const { result, error: errMessage, success } = await this.sendAIAgentMessage(message);

			let response = null;
			
			if (success === false) response = errMessage;
			else if(result?.result.indexOf('"1": {') > -1 && result?.result.indexOf('"2": {') > -1){
				this.setAgentLastMessage(`Pipeline creation executed.`, dataTable);
				return this.controller.parsePipelineCreationContent(result.result);
			}else response = result?.result;

			if (result.fields) {
				const { db_file, fields, actual_query } = result;
				dataTable = this.controller.parseDataToTable(
					fields, response, this.$parent, actual_query, db_file
				);
			} else {
				if ((response || []).length === 0)
					response = 'No data found for the submitted query. Do you want to send another query?';
				else if (String(response).trim() === this.unloadNamespaceMsg) {
					// Auto-reconnect to the chats
					this.$parent.leftMenuProxy.startAIAssistant(true);
					response += `<br>However I've updated myself, let's try again, what's your ask?`
				}
			}

			if (this.isThereAgentMessage === false) this.isThereAgentMessage = true;
			this.setAgentLastMessage(response, dataTable);
		}
	}

	setAgentLastMessage(response, dataTable = null){
		AIAgent.lastAgentParagraph.classList.add('bubble-message-paragraph')
		AIAgent.lastAgentParagraph.innerHTML = dataTable === null ? markdownToHtml(response) : dataTable;
	}

	async sendAIAgentMessage(message){
		if(this.controller.getActiveFlow() === 'data-query')
			return WorkspaceService.sendDataQueryAgentMessage(message);
		if(this.controller.getActiveFlow() === 'pipeline')
			return WorkspaceService.sendPipelineAgentMessage(message);
	}

	createMessageBubble(text, role, alternateRole = null) {
		const row = document.createElement('div');
		row.className = role === 'user' ? 'user-message-row' : 'agent-message-row';

		const bubble = document.createElement('div');
		bubble.className = `message-bubble ${role}-message-bubble`;

		const senderLabel = document.createElement('div');
		senderLabel.className = 'sender-label';
		senderLabel.textContent = role === 'user' ? 'You' : (alternateRole || 'Agent');
		bubble.appendChild(senderLabel);

		const textP = document.createElement('p');
		textP.innerHTML = text;

		if (role === 'agent') AIAgent.lastAgentParagraph = textP;
		if (role === 'user') textP.classList.add('bubble-message-paragraph');

		bubble.appendChild(textP);
		row.appendChild(bubble);
		this.outputContainer.appendChild(row);
		this.scrollToBottom();
	}

	scrollToBottom() {
		setTimeout(() => {
			this.outputContainer.scrollTop = this.outputContainer.scrollHeight;
		}, 200);
	}

	setResizeHandling() {
		const obj = this;
		this.resizeHandle.addEventListener('mousedown', (event) => obj.startDrag(event, obj));
	}

	/** @param {AIAgent} obj  */
	startDrag(e, obj) {
		e.preventDefault();
		obj.isDragging = true;

		obj.startY = e.clientY;
		obj.startHeight = obj.appContainer.offsetHeight;

		document.addEventListener('mousemove', (event) => obj.onDrag(event, obj));
		document.addEventListener('mouseup', (event) => obj.stopDrag(event, obj));

		obj.resizeHandle.style.cursor = 'ns-resize';
		obj.resizeHandle.style.backgroundColor = '#047857'; // Darker color while dragging
	}

	/** @param {AIAgent} obj  */
	onDrag(e, obj) {
		if (!obj.isDragging) return;

		const deltaY = obj.startY - e.clientY;
		let newHeight = obj.startHeight + deltaY;
		const MIN_HEIGHT = 400;
		if (newHeight < MIN_HEIGHT) newHeight = MIN_HEIGHT;
		// Comment to prevent resize for now
		//obj.appContainer.style.height = `${newHeight}px`;
	}

	/** @param {AIAgent} obj  */
	stopDrag(obj) {
		if (!obj.isDragging) return;
		obj.isDragging = false;

		document.removeEventListener('mousemove', () => obj.onDrag(obj));
		document.removeEventListener('mouseup', () => obj.stopDrag(obj));

		document.body.style.cursor = '';
		obj.resizeHandle.style.backgroundColor = '#d1d5db';
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

	hideAgentUI = () => this.$parent.showOrHideAgent();

	setUserPrompt = (content) => {
		document.getElementById('ai-chat-user-input').value = content;
		document.getElementById('ai-chat-user-input').focus();
	}

	async botMessage(event){

		const message = event.target.value;
		this.createMessageBubble(message, 'user');

		event.target.value = '';
		if (!message) return;

		console.log(`You: ${message}`);

		const reply = await this.botInstance.reply("local-user", message);
		console.log(`Bot: ${reply}`);

		return reply;

	}

}