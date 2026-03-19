import { botSubRoutineCall as botRoutineCall, 
    dataQueryFlowMessage, 
    agentOptions as optionsForAgents, 
    usingSecretPrompt as secretPrompt, 
    whatAboutData as aboutData, 
    dontFollowAgentFlow as doNotFollow
} from "./commons.js";
import { dataQueryFlow } from "./dataQueryFlow.js";
import { pipelineFlowMessage } from "./pipelineFlow.js";
import { semanticFlowMessage, semanticSearchFlow } from "./semanticSearchFlow.js";

export const BOT = { lastUserMessage: null, process: 'process-bot-flow-will-proceed' };

window._WorkspaceBOT_ = {};
window._WorkspaceBOT_.getLastUserPrompt = () =>  BOT.lastUserMessage;

window._WorkspaceBOT_.pendingOptions = null;

window._WorkspaceBOT_.setPendingOptions = (context) => 
    window._WorkspaceBOT_.pendingOptions = context;

window._WorkspaceBOT_.getPendingOptions = () => window._WorkspaceBOT_.pendingOptions;

window._WorkspaceBOT_.clearPendingOptions = () => 
    window._WorkspaceBOT_.pendingOptions = null;


export const unkwonRequest = `I didn't understand your request, can you be more clear?`;
export const ifExistingFlowUseIt = `if-existing-flow-use-it`;
export const aiStartSuggestions = `You can say <b>Pipeline</b>, <b>Catalog</b> or <b>Query data</b> to initiate a corresponding flow.`
export const aiStartOptions = `Bellow are some options I can perform for you.`
export const usingSecretPrompt = secretPrompt;

export const dontFollow = { transform: 'transformation' }
export const agentOptions = optionsForAgents;
export const botSubRoutineCall = botRoutineCall;
export const whatAboutData = aboutData;

export const dontFollowAgentFlow = doNotFollow;

export const content = `
! version = 2.0

> object check_data_request javascript
    ${functionDataContent()}
< object

> object check_pending_option javascript
    ${checkPendingOption()}
< object

+ [*] (data|database|db) [*]
- ${botSubRoutineCall}<call>check_data_request</call>


${pipelineFlowMessage}


${dataQueryFlow}


${semanticSearchFlow}


+ [*] (transformation|transform) [*]
- ${dontFollowAgentFlow}Is this about <b>transformation</b> node/step in the <b>pipeline</b>? <b><br>1. Yes<br>2. No</b> <call>setDontFollowAgent "${dontFollow.transform}"</call>

+ (yes|y) no pipeline transformation
- ${dontFollowAgentFlow}But there is no pipeline created in the namespace. Tell me what is this pipeline doing so I can create it.

+ (no|n) no pipeline transformation
- ${dontFollowAgentFlow}Whats is this transformation about then?


// Any other type of query not related to DataQuery of Pipeline
+ *
* <get unknow_count> == 1 => ${ifExistingFlowUseIt}${unkwonRequest} <br><br><p>${aiStartSuggestions}</p> <set unknow_count=2>
* <get unknow_count> == 2 => ${ifExistingFlowUseIt}${unkwonRequest} <br><br><p>${aiStartOptions}</p> <set unknow_count=3><call>displayIAAgentOptions</call>
* <get unknow_count> == 3 => ${ifExistingFlowUseIt}${unkwonRequest}<set unknow_count=1>
- ${ifExistingFlowUseIt}${unkwonRequest} <set unknow_count=1>

`;

function functionDataContent() {
    return `
    const userMessage = _WorkspaceBOT_.getLastUserPrompt().toLowerCase();

    const isPipeline = userMessage.search(/pipeline|pipelines|transformation|transform/);
    const isCreation = userMessage.search(/create|build|craft|do|design|generate|make|setup|set up/);

    if (isPipeline > -1 && isCreation > -1)
        return "${pipelineFlowMessage}";

    return "${dataQueryFlowMessage}";
    `;
}

function functionDataContent1() {
    return `
    const userMessage = _WorkspaceBOT_.getLastUserPrompt().toLowerCase();

    const isPipeline = userMessage.search(/pipeline|pipelines|transformation|transform/);
    const isCreation = userMessage.search(/create|build|craft|do|design|generate|make|setup|set up/);
    const isCatalog  = userMessage.search(/catalog|schema|field|fields|column|columns|mapping|concept|semantic|structure|definition/);
    const isData     = userMessage.search(/fetch|query|get|show|bring|records|rows|items|select|top|list/);
    const isDatabase = userMessage.search(/database|db/);

    if (isPipeline > -1 && isCreation > -1)
        return "${pipelineFlowMessage}";

    if (isDatabase > -1 && isCatalog > -1)
        return "${semanticFlowMessage}";

    if (isDatabase > -1 && isData > -1)
        return "${dataQueryFlowMessage}";

    if (isCatalog > -1)
        return "${semanticFlowMessage}";

    if (isData > -1)
        return "${dataQueryFlowMessage}";

    window._WorkspaceBOT_.setPendingOptions('database');
    return "${dontFollowAgentFlow}Is this about <b>Catalog / Schema</b> or <b>Fetching Data</b>?<br><br><b>1. Catalog / Schema</b><br><b>2. Fetch Data</b><br><b>3. Pipeline</b> <call>setDontFollowAgent \\"database\\"</call>";
    `;
}


function checkPendingOption(){
    return `
    const pending = window._WorkspaceBOT_.getPendingOptions();
    const userMessage = _WorkspaceBOT_.getLastUserPrompt().trim();

    if (pending === 'database') {
        window._WorkspaceBOT_.clearPendingOptions();
        //if (userMessage === '0') return "${semanticFlowMessage}";
        if (userMessage === '2') return "${dataQueryFlowMessage}";
        if (userMessage === '3') return "${pipelineFlowMessage}";
    }

    return "continue";
    `;
}