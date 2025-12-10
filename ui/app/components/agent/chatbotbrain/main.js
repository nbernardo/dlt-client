export const unkwonRequest = `I didn't understand your request, can you be more clear?`;
export const aiStartSuggestions = `You can say <b>Pipeline</b> or <b>Query data</b> to initiate a corresponding flow.`
export const aiStartOptions = `Bellow are some options I can perform for you.`

export const pipelineOption = '(1|one|pipelines|pipeline)';
export const pipelinePrompt0 = '[*] (create|generate|craft|build|do) [*] (pipeline|pipelines) [*]';
export const pipelinePrompt1 = '[*] (what|which|how|when|where) [*] (pipeline|pipelines) [*] [(create|generate|craft|build|do|created|generated|crafted|built|done)] [*]';

export const dataQueryOption = '(2|two|data query|query data|data|query)';
export const dataQueryPrompt0 = '[*] (the tables available|the available tables) [*]';
export const dataQueryPrompt1 = '[*] (show|show me|list|display|what|which|tell me what|tell me which) [*] (tables|table) [*]';
export const dataQueryPrompt2 = '[*] (get me the|get me the list of|get) [*] (tables|table) [*]';
export const dataQueryPrompt3 = '[*] (show|show me|list|display|what|which|tell me what|tell me which) [*] (data|records|table item|items) [*]';

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


// Any other type of query not related to DataQuery of Pipeline
+ *
* <get unknow_count> == 1 => ${unkwonRequest} <br><br><p>${aiStartSuggestions}</p> <set unknow_count=2>
* <get unknow_count> >= 2 => ${unkwonRequest} <br><br><p>${aiStartOptions}</p> <set unknow_count=3><call>displayIAAgentOptions</call>
- ${unkwonRequest} <set unknow_count=1>

`;