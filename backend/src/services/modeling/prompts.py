SEMANTIC_MODEL_PROMPR = """You are a data catalog expert. Classify each column into a semantic concept.

Columns to classify:
{0}

For each column return a JSON array where each object has:
- "table_name": exact table name as given
- "original_column_name": exact column name as given
- "semantic_concept": short snake_case concept (e.g. financial_value, customer_identifier, date, status)
- "confidence_score": float between 0.0 and 1.0
- "description": one sentence explaining what this column likely represents

Return ONLY the JSON array, no preamble, no markdown, no backticks.
"""