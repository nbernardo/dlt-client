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

export const unkwonRequest = `I didn't understand your request, can you be more clear?`;
export const ifExistingFlowUseIt = `if-existing-flow-use-it`;
export const aiStartSuggestions = `You can say <b>Pipeline</b> or <b>Query data</b> to initiate a corresponding flow.`
export const aiStartOptions = `Bellow are some options I can perform for you.`
export const usingSecretPrompt = secretPrompt;

export const dontFollow = { transform: 'transformation' }
export const agentOptions = optionsForAgents;
export const botSubRoutineCall = botRoutineCall;
export const whatAboutData = aboutData;

export const dontFollowAgentFlow = doNotFollow;

export const content = `
! version = 2.0

${pipelineFlowMessage}


${dataQueryFlow}


${semanticSearchFlow}

+ [*] (transformation|transform) [*]
- ${dontFollowAgentFlow}Is this about <b>transformation</b> node/step in the <b>pipeline</b>? <b><br>1. Yes<br>2. No</b> <call>setDontFollowAgent "${dontFollow.transform}"</call>


+ (yes|1) no pipeline transformation
- ${dontFollowAgentFlow}But there is no pipeline created in the namespace. Tell me what is this pipeline doing so I can create it.


+ (no|2) no pipeline transformation
- ${dontFollowAgentFlow}Whats is this transformation about then?


+ [*] (database|db) [*]
- ${dontFollowAgentFlow}Is this about the <b>catalog/schema</b> or <b>fetching data</b>? <br><br><b>1. Catalog / Schema</b><br><b>2. Fetch Data</b> <call>setDontFollowAgent "database"</call>

+ (yes|1) database catalog
- ${semanticFlowMessage} <call>setSemanticFlow</call>

+ (no|2) database fetch
- ${dataQueryFlowMessage} <call>setDataQueryFlow</call>


// Any other type of query not related to DataQuery of Pipeline
+ *
* <get unknow_count> == 1 => ${ifExistingFlowUseIt}${unkwonRequest} <br><br><p>${aiStartSuggestions}</p> <set unknow_count=2>
* <get unknow_count> == 2 => ${ifExistingFlowUseIt}${unkwonRequest} <br><br><p>${aiStartOptions}</p> <set unknow_count=3><call>displayIAAgentOptions</call>
* <get unknow_count> == 3 => ${ifExistingFlowUseIt}${unkwonRequest}<set unknow_count=1>
- ${ifExistingFlowUseIt}${unkwonRequest} <set unknow_count=1>



`;