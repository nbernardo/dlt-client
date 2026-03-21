import { bringMeRequest, dataQueryFlowMessage, showMeRequest } from "./commons.js";

export const dataQueryOption = '(2|two|data query|query data)';
export const dataQueryPrompt0 = '[*] (the tables available|the available tables) [*]';
export const dataQueryPrompt1 = `[*] ${showMeRequest} [*] (tables|table) [*]`;
export const dataQueryPrompt2 = `[*] ${bringMeRequest} [*] (tables|table|from) [(table)] [*]`;
export const dataQueryPrompt3 = `[*] ${showMeRequest} [*] (data|records|table item|items) [*]`;
export const dataQueryPrompt4 = `[*] [${showMeRequest}] [*] (data|database|db) [*] (tables|items) [*]`;

//export const dataQueryPrompt0 = `[*] (show data|show me data|list data|display data|what data|whats data|which data|tell me what data|tell me whats data|tell me which data) [*]`;
//export const dataQueryPrompt1 = `[*] (show records|show me records|list records|display records|what records|whats records|which records|tell me what records|tell me whats records|tell me which records) [*]`;
//export const dataQueryPrompt2 = `[*] (show table item|show me table item|list table item|display table item|what table item|whats table item|which table item|tell me what table item|tell me whats table item|tell me which table item) [*]`;
//export const dataQueryPrompt3 = `[*] (show items|show me items|list items|display items|what items|whats items|which items|tell me what items|tell me whats items|tell me which items) [*]`;
//export const dataQueryPrompt4 = `[*] (show tables|show me tables|list tables|display tables|what tables|whats tables|which tables|tell me what tables|tell me whats tables|tell me which tables) [*]`;

export const dataQueryFlow = `
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
`