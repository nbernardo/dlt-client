space = '<br>&nbsp;&nbsp;&nbsp;&nbsp;- '
labels = {
    'PT': {
        'TBL_MAIN_HEAD': 'Tabelas processadas',
        'TBL_TABLE_HEAD_LBL': 'Tabela',
        'TBL_REC_HEAD_LBL': 'Total de registos',
        'PPLINE_SUCCESS': 'Pipeline executado com sucesso',
        'PPLINE_START': 'Iníciou em:',
        'PPLINE_END': 'Terminou em:',
        'PPLINE_TIME': 'Timestamp:',
        'SBJCT_PREFX': 'Concluído com sucesso | ',
        'SBJCT_SFFIX': '',      
        'PPLINE_FAIL_EXEC_SBJ_SFX': '',      
        'PPLINE_FAIL_EXEC_SBJ_PFX': 'Erro ao executar o pipeline',
        'PPLINE_FAIL_TXT': f''''
            <h2>Houve um erro na execucao do pipeline: </h2>{space}Pipeline: {{pp_name}}{space}Iníciou em: {{sdate}}{space}Timesnamp: {{tstamp}}
        ''',
    },
    'EN': {
        'TBL_MAIN_HEAD': 'Ingested tables',
        'TBL_TABLE_HEAD_LBL': 'Table name',
        'TBL_REC_HEAD_LBL': 'Total records',
        'PPLINE_SUCCESS': 'Pipeline execution success',
        'PPLINE_START': 'Start time:',
        'PPLINE_END': 'End time:',
        'PPLINE_TIME': 'Timestamp:',
        'SBJCT_PREFX': 'Success | ',
        'SBJCT_SFFIX': 'Execution',
        'PPLINE_FAIL_EXEC_SBJ_SFX': 'pipeline', 
        'PPLINE_FAIL_EXEC_SBJ_PFX': 'Failed the execution of', 
        'PPLINE_FAIL_TXT': f''''
            <h2>There was an error while running the pipeline:</h2>{space}Pipeline: {{pp_name}}{space}Start date: {{sdate}}{space}Timesnamp: {{tstamp}}
        ''',
    },
}