import { agentOptions, botSubRoutineCall, bringMeRequest, dontFollowAgentFlow, showMeRequest, usingSecretPrompt } from "./commons.js";

function functionSecretContent(type, typeId){
    return `
    const userMessage = _WorkspaceBOT_.getLastUserPrompt();

    const isSecrets = userMessage.search(/secrets|secret|connection/);
    const isUseSecret = userMessage.search(/use|used|using|assign|set|put/);

    if((isUseSecret > -1) && (isUseSecret < isSecrets))
        return "${agentOptions.pipeline}${usingSecretPrompt}";

    return "${dontFollowAgentFlow}showSerets %sep% Follow the list of ${type} secrets: %sep% ${typeId}";
    `;
}

export const pipelineOption = '(1|one|pipelines|pipeline)';
export const pipelinePrompt0 = '[*] (create|generate|craft|build|do) [*] (pipeline|pipelines) [*]';
export const pipelinePrompt1 = '[*] (what|whats|which|how|when|where) [*] (pipeline|pipelines) [*] [(create|generate|craft|build|do|created|generated|crafted|built|done)] [*]';
export const pipelinePrompt2 = '[*] (transformation|pipeline|pipelines) [*] (transformation|pipeline|pipelines) [*]';
export const pipelinePrompt3 = '[*] (data|transformation) [*] (transformation|data) [*]';

export const secretAsk0 = '[*] (the secrets|the secret|the connection|the connections|the available secrets|the available secret|the available connection|the available connections) [*]';
export const secretAsk1 = `[*] [${bringMeRequest}] [*] (secret|secrets|connection|connectios) [*]`;
export const secretAsk2 = `[*] [${showMeRequest}] [*] (secret|secrets|connection|connectios) [*]`;

export const secretAskPipeline1 = `(pipeline secret|pipeline secrets|pipelines secret|pipelines secrets|secret pipeline|secret pipelines)`;
export const secretAskPipeline2 = `(get me the pipeline secret|get me the pipeline secrets|get me the pipelines secret|get me the pipelines secrets|get me the secret pipeline|get me the secret pipelines)`;
export const secretAskPipeline3 = `(show me pipeline secret|show me pipeline secrets|show me pipelines secret|show me pipelines secrets|show me secret pipeline|show me secret pipelines)`;

export const secretAsk3 = `[*] [${showMeRequest}] [*] (service|api) [*] (secret|secrets|connection|connectios) [*]`;
export const secretAsk4 = `[*] [${showMeRequest}] [*] (secret|secrets|connection|connectios) [*] (service|api) [*]`;
export const secretAsk5 = `[*] [${showMeRequest}] [*] (database|db) [*] (secret|secrets|connection|connectios) [*]`;
export const secretAsk6 = `[*] [${showMeRequest}] [*] (secret|secrets|connection|connectios) [*] (database|db) [*]`;

export const pipelineFlowMessage = `Let's go for pipeline flow. ${agentOptions.pipeline}`;

export const pipeline = `
// Pipeline AI Agent flow redirection
+ * ${pipelineOption} *
- ${pipelineFlowMessage} <call>setPipelineFlow</call>

+ ${pipelineOption} *
- ${pipelineFlowMessage} <call>setPipelineFlow</call>

+ * ${pipelineOption}
- ${pipelineFlowMessage} <call>setPipelineFlow</call>

+ ${pipelineOption}
- ${pipelineFlowMessage} <call>setPipelineFlow</call>

+ ${pipelinePrompt0}
- ${pipelineFlowMessage} <call>setPipelineFlow</call>

+ ${pipelinePrompt1}
- ${pipelineFlowMessage} <call>setPipelineFlow</call>

+ ${pipelinePrompt2}
- ${pipelineFlowMessage} <call>setPipelineFlow</call>

+ ${pipelinePrompt3}
- ${pipelineFlowMessage} <call>setPipelineFlow</call>

// Secrets questions/asks
> object check_secret_request_api javascript
    ${functionSecretContent('API',2)}
< object

> object check_secret_request_db javascript
    ${functionSecretContent('Database',1)}
< object

> object check_secret_request javascript
    ${functionSecretContent('','all')}
< object


+ ${secretAskPipeline1}
- ${pipelineFlowMessage} PPLINE <call>setPipelineFlow</call>

+ ${secretAskPipeline2}
- ${pipelineFlowMessage} PPLINE <call>setPipelineFlow</call>

+ ${secretAskPipeline3}
- ${pipelineFlowMessage} PPLINE <call>setPipelineFlow</call>

+ ${secretAsk3}
- ${botSubRoutineCall}<call>check_secret_request_api</call>

+ ${secretAsk4}
- ${botSubRoutineCall}<call>check_secret_request_api</call>

+ ${secretAsk5}
- ${botSubRoutineCall}<call>check_secret_request_db</call>

+ ${secretAsk6}
- ${botSubRoutineCall}<call>check_secret_request_db</call>

+ ${secretAsk0}
- ${botSubRoutineCall}<call>check_secret_request</call>

+ ${secretAsk1}
- ${botSubRoutineCall}<call>check_secret_request</call>

+ ${secretAsk2}
- ${botSubRoutineCall}<call>check_secret_request</call>
`;