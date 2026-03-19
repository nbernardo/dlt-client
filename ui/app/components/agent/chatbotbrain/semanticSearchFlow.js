import { agentOptions } from "./commons.js";

export const semanticOption = '(semantic model|data catalog|catalog of data|tables mapping|field mapping|column mapping|column concept|column meaning|semantic catalog|database schema|table schema|tables schema)';

export const semanticPrompt0 = `[*] (data catalog|data schema) [*]`;
export const semanticPrompt1 = `[*] (catalog) [*] (data|tables|columns|fields|schema) [*]`;
export const semanticPrompt2 = `[*] (tables|fields|columns|database|db) [*] (mapping|schema) [*]`;
export const semanticPrompt3 = `[*] (schema) [*] (table|tables|database|db|use|should) [*]`;
export const semanticPrompt4 = `[*] (semantic) [*] (model|concept|field|column|catalog) [*]`;
export const semanticPrompt5 = `[*] (meaning|concept|classification) [*] (column|field|data) [*]`;
export const semanticPrompt6 = `[*] (table) [*] (column|columns|fields|field) [*]`;

export const semanticFlowMessage = `Let's query the semantic catalog. ${agentOptions.semantic}`;

export const semanticSearchFlow = `
// Semantic Model / Catalog AI Agent flow redirection
+ * ${semanticOption} *
- ${semanticFlowMessage} <call>setSemanticFlow</call>

+ ${semanticOption} *
- ${semanticFlowMessage} <call>setSemanticFlow</call>

+ * ${semanticOption}
- ${semanticFlowMessage} <call>setSemanticFlow</call>

+ ${semanticOption}
- ${semanticFlowMessage} <call>setSemanticFlow</call>

+ ${semanticPrompt0}
- ${semanticFlowMessage} <call>setSemanticFlow</call>

+ ${semanticPrompt1}
- ${semanticFlowMessage} <call>setSemanticFlow</call>

+ ${semanticPrompt2}
- ${semanticFlowMessage} <call>setSemanticFlow</call>

+ ${semanticPrompt3}
- ${semanticFlowMessage} <call>setSemanticFlow</call>

+ ${semanticPrompt4}
- ${semanticFlowMessage} <call>setSemanticFlow</call>

+ ${semanticPrompt5}
- ${semanticFlowMessage} <call>setSemanticFlow</call>

+ ${semanticPrompt6}
- ${semanticFlowMessage} <call>setSemanticFlow</call>
`;