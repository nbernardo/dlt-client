import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { Assets } from "../../../@still/util/componentUtil.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";
import { Workspace } from "../workspace/Workspace.js";

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

	/** @type { HTMLParagraphElement } */
	static lastAgentParagraph;

	async stBeforeInit() {
		await Assets.import({ path: '/app/assets/css/agent.css' });
	}

	async stAfterInit() {		
		this.appContainer = document.querySelector('.ai-app-container');
		this.outputContainer = document.getElementById(this.outputContainerId);
		this.resizeHandle = document.querySelector('.ai-agent-height-resize');
		this.resizeHandle.style.backgroundColor = '#047857'
		this.setResizeHandling();		
		await this.startNewAgent();
	}

	async startNewAgent() {
		await WorkspaceService.startChatConversation();
	}

	async sendChatRequest(event) {
		if (event.key === 'Enter') {
			event.preventDefault();
			const message = event.target.value;
			event.target.value = '';
			this.createMessageBubble(message, 'user');
			this.scrollToBottom();
			this.createMessageBubble(this.loadingContent(), 'agent');
			const { result } = await WorkspaceService.sendAgentMessage(message);

			let response = result?.result;
			
			if((response || []).length === 0)
				response = 'No data found for the submitted query. Do you want to send another query?';

			if(this.isThereAgentMessage === false) this.isThereAgentMessage = true;
			
			AIAgent.lastAgentParagraph.classList.add('bubble-message-paragraph')
			AIAgent.lastAgentParagraph.innerHTML = response;
		}
	}

	createMessageBubble(text, role) {
		const row = document.createElement('div');
		row.className = role === 'user' ? 'user-message-row' : 'agent-message-row';

		const bubble = document.createElement('div');
		bubble.className = `message-bubble ${role}-message-bubble`;

		const senderLabel = document.createElement('div');
		senderLabel.className = 'sender-label';
		senderLabel.textContent = role === 'user' ? 'You' : 'Agent';
		bubble.appendChild(senderLabel);

		const textP = document.createElement('p');
		textP.innerHTML = text;

		if(role === 'agent') AIAgent.lastAgentParagraph = textP;
		if(role === 'user') textP.classList.add('bubble-message-paragraph');

		bubble.appendChild(textP);
		row.appendChild(bubble);
		this.outputContainer.appendChild(row);

		return textP;
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

	loadingContent(){
		return `
			<div class="mini-loader-container">
				<div class="mini-loader-dot" style="background: black;"></div>
				<div class="mini-loader-dot" style="background: black;"></div>
				<div class="mini-loader-dot" style="background: black;"></div>
			</div>
		`;
	}

}