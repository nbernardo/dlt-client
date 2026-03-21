export const whatAboutData = 'What do you want about the data?';
export const bringMeRequest = '(get me the|get me the list of|get|bring me|fetch|fetch me)';
export const showMeRequest = '(show|show me|list|display|what|whats|which|tell me what|tell me whats|tell me which)';
export const agentOptions = { 
    pipeline: `'it-is-pipeline-flow'`, 
    dataQuery: `'it-is-data_query-flow'`, 
    semantic:  `'it-is-semantic-flow'`
}

export const botSubRoutineCall = `bot-routing-call-only`;
export const usingSecretPrompt = `using-secret-prompt`;
export const dataQueryFlowMessage = `${whatAboutData} ${agentOptions.dataQuery}`;
export const dontFollowAgentFlow = `dont-follow-agent-flow`;