import { markdownToHtml } from "./Markdown.js";

/** @type { HTMLParagraphElement } */
export let lastAgentParagraph = null;

export const AIUtil = {

    aiAgentFlow: null,

    aiResponseMessageAnchor: null,

    AgentFlowType: { GENERIC: 'generic', ANALYTICS: 'analitics' },

    createMessageBubble: (text, role, alternateRole = null, mainContainer = null) => {
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

        if (role === 'agent') lastAgentParagraph = textP;
        if (role === 'user') textP.classList.add('bubble-message-paragraph');

        bubble.appendChild(textP);
        row.appendChild(bubble);
        if(mainContainer) mainContainer.appendChild(row);

        return row;
    },

    setAgentLastMessage: (response, dataTable = null, anchor = false, messagesContainer = null) => {
        AIUtil.aiResponseMessageAnchor = null;
        lastAgentParagraph.classList.add('bubble-message-paragraph');
        let finalContent = dataTable === null ? markdownToHtml(response) : dataTable;
        if(anchor){
            AIUtil.aiResponseMessageAnchor = 'lastMessageAnchor'+UUIDUtil.newId();
        }
        finalContent = `<h2 id="${AIUtil.aiResponseMessageAnchor}"></h2>${finalContent}`;
        lastAgentParagraph.innerHTML = finalContent;

        if(AIUtil.aiResponseMessageAnchor !== null) scrollToBottom(anchor, messagesContainer);
    },

    scrollToBottom: (anchor, obj) => scrollToBottom(anchor, obj),

    loadingContent: () => {
		return `
			<div class="mini-loader-container">
				<div class="mini-loader-dot" style="background: black;"></div>
				<div class="mini-loader-dot" style="background: black;"></div>
				<div class="mini-loader-dot" style="background: black;"></div>
			</div>
		`;
	}
}


export function scrollToBottom(ancor = false, obj){
	setTimeout(() => {
		const element = document.getElementById(AIUtil.aiResponseMessageAnchor);
		if(ancor && element)
			element.scrollIntoView({behavior: 'smooth', block: 'start'}); 
		else obj.scrollTop = obj.scrollHeight;
	}, 200);
}