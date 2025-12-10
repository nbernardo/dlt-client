export const unkwonRequest = `I didn't understand your request, can you be more clear?`;

export const content = `
! version = 2.0

// Pipeline AI Agent flow redirection

+ * (1|one|pipelines|pipeline) *
- Let's go for pipeline flow 1. <call>setPipelineFlow</call>

+ (1|one|pipelines|pipeline) *
- Let's go for pipeline flow 2. <call>setPipelineFlow</call>

+ * (1|one|pipelines|pipeline)
- Let's go for pipeline flow 3. <call>setPipelineFlow</call>

+ (1|one|pipelines|pipeline)
- Let's go for pipeline flow 4. <call>setPipelineFlow</call>


// Data Query AI Agent flow redirection

+ * (2|two|data query|query data) *
- Whats do you want about the data? <call>setDataQueryFlow</call>

+ (2|two|data query|query data) *
- Whats do you want about the data? <call>setDataQueryFlow</call>

+ * (2|two|data query|query data)
- Whats do you want about the data? <call>setDataQueryFlow</call>

+ (2|two|data query|query data|data|query)
- Whats do you want about the data? <call>setDataQueryFlow</call>



+ *
- ${unkwonRequest}

`;