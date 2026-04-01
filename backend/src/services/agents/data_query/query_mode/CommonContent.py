class CommonContent:

    complement = 'Rules:\n- Only output final SQL\n- No explanation\n- Use provided schema only'
    complement += '\n- Match the columns name appropriately as per the table it relates\n- Be precise and minimal\n\n'
    complement += '\n- If the tables are prefixed (e.g. db.table_name) do not remove them in the final query'
    complement += '\nDo not try to craft a query that do not make sense with the user request and the context. Rather ask clarification'
    complement += '\nOnly ask clarification if Retry >2'

    no_query_generation_rule = "\nRules if note final SQL query:\nIf you need clarification ask prefixing with 'CLARIFY:' only if Retry is > 2."

    explain_complement = 'Rules:\n- Only output the explanation\n- Use provided schema only'
    explain_complement += '\n- If needed put the details in the explanation\n- Be precise and minimal\n\n'

    def system_prompt(language = 'PT'):
        return 'Você é um Analista de Dados.' if language == 'PT' else 'You are a Data Analyst.'
