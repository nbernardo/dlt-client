export const BOT = { lastUserMessage: null };

window._WorkspaceBOT_ = {};
window._WorkspaceBOT_.getLastUserPrompt = () =>  BOT.lastUserMessage;

export const unkwonRequest = `I didn't understand your request, can you be more clear?`;
export const ifExistingFlowUseIt = `if-existing-flow-use-it`;
export const dontFollowAgentFlow = `dont-follow-agent-flow`;
export const botSubRoutineCall = `bot-routing-call-only`;
export const aiStartSuggestions = `You can say <b>Pipeline</b> or <b>Query data</b> to initiate a corresponding flow.`
export const aiStartOptions = `Bellow are some options I can perform for you.`
export const usingSecretPrompt = `using-secret-prompt`

const bringMeRequest = '(get me the|get me the list of|get|bring me|fetch|fetch me)';
const showMeRequest = '(show|show me|list|display|what|whats|which|tell me what|tell me whats|tell me which)';

export const dontFollow = { transform: 'transformation' }

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

export const pipelineOption = '(1|one|pipelines|pipeline)';
export const pipelinePrompt0 = '[*] (create|generate|craft|build|do) [*] (pipeline|pipelines) [*]';
export const pipelinePrompt1 = '[*] (what|whats|which|how|when|where) [*] (pipeline|pipelines) [*] [(create|generate|craft|build|do|created|generated|crafted|built|done)] [*]';
export const pipelinePrompt2 = '[*] (transformation|pipeline|pipelines) [*] (transformation|pipeline|pipelines) [*]';
export const pipelinePrompt3 = '[*] (data|transformation) [*] (transformation|data) [*]';

export const dataQueryOption = '(2|two|data query|query data|data|query)';
export const dataQueryPrompt0 = '[*] (the tables available|the available tables) [*]';
export const dataQueryPrompt1 = `[*] ${showMeRequest} [*] (tables|table) [*]`;
export const dataQueryPrompt2 = `[*] ${bringMeRequest} [*] (tables|table|from) [(table)] [*]`;
export const dataQueryPrompt3 = `[*] ${showMeRequest} [*] (data|records|table item|items) [*]`;
export const dataQueryPrompt4 = `[*] [${showMeRequest}] [*] (data|database|db) [*] (table|tables|items) [*]`;

export const agentOptions = { pipeline: `'it-is-pipeline-flow'`, dataQuery: `'it-is-data_query-flow'` }

export const pipelineFlowMessage = `Let's go for pipeline flow. ${agentOptions.pipeline}`;
export const dataQueryFlowMessage = `Whats do you want about the data? ${agentOptions.dataQuery}`;


export const content = `
! version = 2.0


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


// Data Query AI Agent flow redirection
+ * ${dataQueryOption} *
- ${dataQueryFlowMessage} <call>setDataQueryFlow</call>

+ ${dataQueryOption} *
- ${dataQueryFlowMessage} <call>setDataQueryFlow</call>

+ * ${dataQueryOption}
- ${dataQueryFlowMessage} <call>setDataQueryFlow</call>

+ ${dataQueryOption}
- ${dataQueryFlowMessage} <call>setDataQueryFlow</call>

+ ${dataQueryPrompt0}
- ${dataQueryFlowMessage} <call>setDataQueryFlow</call>

+ ${dataQueryPrompt1}
- ${dataQueryFlowMessage} <call>setDataQueryFlow</call>

+ ${dataQueryPrompt2}
- ${dataQueryFlowMessage} <call>setDataQueryFlow</call>

+ ${dataQueryPrompt3}
- ${dataQueryFlowMessage} <call>setDataQueryFlow</call>

+ ${dataQueryPrompt4}
- ${dataQueryFlowMessage} <call>setDataQueryFlow</call>


+ [*] (transformation|transform) [*]
- ${dontFollowAgentFlow}Is this about <b>transformation</b> node/step in the <b>pipeline</b>? <b><br>1. Yes<br>2. No</b> <call>setDontFollowAgent "${dontFollow.transform}"</call>


+ (yes|1) no pipeline transformation
- ${dontFollowAgentFlow}But there is no pipeline created in the namespace. Tell me what is this pipeline doing so I can create it.


+ (no|2) no pipeline transformation
- ${dontFollowAgentFlow}Whats is this transformation about then?


+ [*] (database|db) [*]
- ${dontFollowAgentFlow}What do you want about database?


// Any other type of query not related to DataQuery of Pipeline
+ *
* <get unknow_count> == 1 => ${ifExistingFlowUseIt}${unkwonRequest} <br><br><p>${aiStartSuggestions}</p> <set unknow_count=2>
* <get unknow_count> == 2 => ${ifExistingFlowUseIt}${unkwonRequest} <br><br><p>${aiStartOptions}</p> <set unknow_count=3><call>displayIAAgentOptions</call>
* <get unknow_count> == 3 => ${ifExistingFlowUseIt}${unkwonRequest}<set unknow_count=1>
- ${ifExistingFlowUseIt}${unkwonRequest} <set unknow_count=1>


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


function functionSecretContent(type, typeId){
    return `
    const userMessage = _WorkspaceBOT_.getLastUserPrompt();

    const isSecrets = userMessage.search(/secrets|secret|connection/);
    const isUseSecret = userMessage.search(/use|used|using/);

    if((isUseSecret > -1) && (isUseSecret < isSecrets))
        return "${agentOptions.pipeline}${usingSecretPrompt}";

    return "${dontFollowAgentFlow}showSerets %sep% Follow the list of ${type} secrets: %sep% ${typeId}";
    `;
}