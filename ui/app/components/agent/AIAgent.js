import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { Assets } from "../../../@still/util/componentUtil.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";
import { AIAgentController } from "../../controller/AIAgentController.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";
import { markdownToHtml } from "../../util/Markdown.js";
import { Workspace } from "../workspace/Workspace.js";
import { agentOptions, aiStartOptions, BOT, botSubRoutineCall, content as chatBotBrain, dontFollow, dontFollowAgentFlow, ifExistingFlowUseIt, unkwonRequest, usingSecretPrompt, whatAboutData } from "./chatbotbrain/main.js";

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
    /** @Prop */ lastMessageAnchor = null;

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
	/** @Prop */ dontFollowAgentFlag = null;

	async stBeforeInit() {
		await Assets.import({ path: '/app/assets/css/agent.css' });
		await Assets.import({ path: 'https://unpkg.com/rivescript@latest/dist/rivescript.min.js', type:'js' });

		setTimeout(async () => {
			this.botInstance = new RiveScript();
			this.controller.setupBotSubRoutine(this);
			await this.botInstance.stream(chatBotBrain);
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
		this.controller.agentInstance = this;
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

			this.controller.displayInitialChatOptions(this);
			this.startedInstance = await WorkspaceService.startChatConversation();
			if(this.startedInstance?.connection === false){
				this.createMessageBubble(`<div class="agent-no-start-error">Invalid API Key<br><br>Your conversation will be very limited to the chatbot capabilities.</div>`, 'agent', 'DLT Workspace');
				this.startedInstance = null;
				return this.resizeHandle.style.backgroundColor = '#ff0000ff';
			}
			
			if(this.startedInstance.success === false && this.startedInstance.error.includes(': Connection error.')){
				this.createMessageBubble(`<div class="agent-no-start-error">${this.startedInstance.error}</div>`, 'agent', 'DLT Workspace');
				this.startedInstance = null;
				return this.resizeHandle.style.backgroundColor = '#ff0000ff';
			}

			this.resizeHandle.style.backgroundColor = '#047857';
			
		} catch (error) { }
	}

	setAgentFlow(flowName){
		this.createMessageBubble(flowName, 'user');
		if(this.controller.getActiveFlow() === null){
			const initMessage = this.controller.initAgentActiveFlow(flowName);
			this.createMessageBubble(`${initMessage}`, 'agent', 'DLT Workspace');
		}else
			this.controller.setAgentFlow(flowName);
	};

	async sendChatRequest(event) {
		if (event.key === 'Enter') {

			event.preventDefault();
			let { message, botResponse } = this.controller.agentPreRoute(event.target.value);
			
			if(botResponse != '' && botResponse != usingSecretPrompt){
				botResponse = await this.botMessage(event);
				if(botResponse.includes(aiStartOptions)) return;
	
				const cannotContinue = (botResponse.includes(unkwonRequest) || botResponse.includes(dontFollowAgentFlow))
				const botFunctionCall = botResponse.includes(botSubRoutineCall);
				const useSecretPrompt = botResponse.includes(usingSecretPrompt);
				const stickToPrevFlow = botResponse.includes(ifExistingFlowUseIt);
	
				botResponse = this.controller.setAgentRoute(botResponse);
	
				// In case there is function call bot instruction this call will handle it
				this.controller.handleBotFunctionCall(this, botResponse);
	
				const isFlowNotSet = this.controller.getActiveFlow() == null;
	
				if(useSecretPrompt){ /** continue */ }
				else if((cannotContinue && isFlowNotSet) || botFunctionCall){
					const content = botFunctionCall ? this.controller.loadingContent() : botResponse;
					if(!(stickToPrevFlow && this.controller.getActiveFlow() != null))
						return this.createMessageBubble(content, 'agent', 'DLT Workspace');
				}
			}
			
			message = this.augmentAgentPerception(botResponse, message);
			if(botResponse == '' || botResponse.includes(usingSecretPrompt)){
				this.createMessageBubble(event.target.value, 'user');
				event.target.value = '';
			}
			
			let dataTable = null, response = null;						
			if(this.startedInstance === null){
				this.startNewAgent(true); /** This will retry to connect with the Agent Backend */

				/** Because retry took place once, if it successfull connects, then the startedInstance won't be null */
				if(this.startedInstance === null){
					const result = "No agent was initiated since you don't/didn't have data in the namespace. I can update myself if you ask.";
					return this.createMessageBubble(`<div class="agent-no-start-msg">${result}</div>`, 'agent', 'DLT Workspace');
				}
			}

			this.createMessageBubble(this.controller.loadingContent(), 'agent');
			this.sentMessagesCount = this.sentMessagesCount.value + 1;

			const { result, error: errMessage, success } = await this.sendAIAgentMessage(message);
			
			if (success === false) response = errMessage;
			else if(result?.result.indexOf('"1": {') > -1 && result?.result.indexOf('"2": {') > -1){
				this.setAgentLastMessage(`Pipeline creation executed.`, dataTable);
				await this.controller.parsePipelineCreationContent(result.result);
				// Reset this controller variable for the next prompt
				this.controller.whatCodeTemplateType = { source: null, target: null  };
				return;
			}else response = result?.result;

			if (result.fields) {
				const { db_file, fields, actual_query } = result;
				dataTable = this.controller.parseDataToTable(
					fields, response, this.$parent, actual_query, db_file
				);
			} else {
				if ((response || []).length === 0)
					response = 'Your request was not processed, do you want to be clear about your it?';
				else if (String(response).trim() === this.unloadNamespaceMsg) {
					// Auto-reconnect to the chats
					this.$parent.leftMenuProxy.startAIAssistant(true);
					response += `\nHowever I've updated myself, let's try again, what's your ask?`
					setTimeout(() => this.scrollToBottom());
				}
			}

			if (this.isThereAgentMessage === false) this.isThereAgentMessage = true;
			this.setAgentLastMessage(response, dataTable);
		}
	}

	augmentAgentPerception(botAnswer, message){
		
		const useDbSchema = message.search(/db\s{0,}schema|schema/i);
		const dataFlow = botAnswer.includes(agentOptions.dataQuery) || botAnswer.includes(whatAboutData);

		const msgHasBothPipelineAndDB = 
			message.search(/dbname|database|database[.w]*\s*name|name[.w]*\s*database/) >= 0
			&& message.search(/pipeline|source|destination|output|input/) >= 0;

		const { isChangingPipeline, notCreating, isCreatingPipeline } = this.checkPromptAction(message);
		this.controller.whatCodeTemplateType = this.checkCodeNodeTemplate(message);

		if(dataFlow && !(useDbSchema >= 0) && !msgHasBothPipelineAndDB)
			message = 'Get from DB Schema\n' + message;

		if(isChangingPipeline && notCreating)
			message = `${message}, you'll update the bellow pipeline JSON and responde with the updated version\n:${this.controller.lastPipeline}`;
		
		if(isCreatingPipeline)
			message = `${message}, you'll not use the previous pipeline but create a new o`;
		
		if(botAnswer.includes(usingSecretPrompt)){
			this.controller.setAgentFlow(this.controller.flowPrefix.pipeline);
			const augmentedRequest = `ROUTE(pipeline-agent)\n\nYOU'LL CONSIDER THE BELLOW JSON OF SECRETS MAP:\n${JSON.stringify(this.controller.secretsData)}\nAND DO THE FOLLOWING:\n${message}`;
			return augmentedRequest.replace(usingSecretPrompt, '');
		}
		return message;
	}

	checkPromptAction(message){
		const notCreate1 = message.search(/pipeline[\w]*\s*(create|build|construct|creation)/i) >= 0;
		const notCreate2 = message.search(/(create|build|construct|creation)\s*[\w]*\s*pipeline/i) >= 0;
		const notCreating = !notCreate1 && !notCreate2;
		const isChangingPipeline = message.search(/change(ed)*|assign(ed)*|replace(ed)*|add(ed)*|remove(d)*|modify|modified|will be|is/i) >= 0;
		return { notCreating, isChangingPipeline, isCreatingPipeline: !notCreating };
	}

	checkCodeNodeTemplate(message){
		const kafkaSource1 = message.search(/(kafka|mongo)\s*[\w]*\s*(get(ting|ing)*|pull(s|ing)*|fetch(ed|ing)*|sourc(e|ed|ing)*)/i) >= 0;
		const kafkaSource2 = message.search(/(get(ting|ing)*|pull(s|ing)*|fetch(ed|ing)*|sourc(e|ed|ing)*)\s*[\w]*\s*(kafka|mongo)/i) >= 0;

		/** TODO: Use those when the implementation of target code is in place */
		const kafkaTgt1 = message.search(/(kafka|mongo)\s*[\w]*\s*(wri(te|tes|ting|tting)*|dump(s|ping|ing)*|destination|sen(d|ds|t|ing)|put(s|ing)*)/i) >= 0;
		const kafkaTgt2 = message.search(/(wri(te|tes|ting|tting)*|dump(s|ping|ing)*|destination|sen(d|ds|t|ing)|put(s|ing)*)\s*[\w]*\s*(kafka|mongo)/i) >= 0;

		if(kafkaSource1 || kafkaSource2)
			return { source: message.search(/kafka/i) >= 0 ? 'kafka' : 'mongo', target: null };
		return {}
	}

	setAgentLastMessage(response, dataTable = null, anchor = false){
		this.lastMessageAnchor = null;
		AIAgent.lastAgentParagraph.classList.add('bubble-message-paragraph');
		let finalContent = dataTable === null ? markdownToHtml(response) : dataTable;
		if(anchor){
			this.lastMessageAnchor = 'lastMessageAnchor'+UUIDUtil.newId();
		}
		finalContent = `<h2 id="${this.lastMessageAnchor}"></h2>${finalContent}`;
		AIAgent.lastAgentParagraph.innerHTML = finalContent;

		if(this.lastMessageAnchor !== null) this.scrollToBottom(true);
	}

	async sendAIAgentMessage(message){
		if(this.controller.getActiveFlow() === this.controller.flowPrefix.data)
			return WorkspaceService.sendDataQueryAgentMessage(message);
		if(this.controller.getActiveFlow() === this.controller.flowPrefix.pipeline)
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
		this.scrollToBottom(false);
	}

	scrollToBottom(ancor = false) {
		setTimeout(() => {
			const element = document.getElementById(this.lastMessageAnchor);
			if(ancor && element)
				element.scrollIntoView({behavior: 'smooth', block: 'start'}); 
			else this.outputContainer.scrollTop = this.outputContainer.scrollHeight;
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

	hideAgentUI = () => this.$parent.showOrHideAgent();
	setUserPrompt = (content) => this.controller.setUserPrompt(content)

	/** @returns { Promise<String> } */
	async botMessage(event){

		let message = event.target.value, complementMessage = '';
		BOT.lastUserMessage = message;

		if(this.dontFollowAgentFlag == dontFollow.transform 
			&& this.$parent.checkActiveDiagram() === false){
			complementMessage = ' no pipeline transformation'
		}

		this.createMessageBubble(message, 'user');

		event.target.value = '';
		if (!message) return;

		console.log(`You: ${message}`);

		const reply = await this.botInstance.reply("local-user", message+''+complementMessage);
		console.log(`Bot: ${reply}`);

		return reply;

	}

	shrinkChatSize = () => this.controller.shrinkChatSize()

}