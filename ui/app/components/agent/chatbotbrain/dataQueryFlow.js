import { bringMeRequest, dataQueryFlowMessage, showMeRequest } from "./commons.js";

export const dataQueryOption = '(2|two|data query|query data|data|query)';
export const dataQueryPrompt0 = '[*] (the tables available|the available tables) [*]';
export const dataQueryPrompt1 = `[*] ${showMeRequest} [*] (tables|table) [*]`;
export const dataQueryPrompt2 = `[*] ${bringMeRequest} [*] (tables|table|from) [(table)] [*]`;
export const dataQueryPrompt3 = `[*] ${showMeRequest} [*] (data|records|table item|items) [*]`;
export const dataQueryPrompt4 = `[*] [${showMeRequest}] [*] (data|database|db) [*] (table|tables|items) [*]`;

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