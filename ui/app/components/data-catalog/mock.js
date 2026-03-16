export const dataCatalogsMock = {

    PIPELINES: {
    p1: {
        name: 'orcl_testing_to_mssql',
        tables: {
        'CUSTOMERS': {
            columns: [
            { name:'CUSTOMER_ID',  type:'integer', version:1, deleted:0, semantic:'customer_identifier', sem_source:'rule', validated:1 },
            { name:'FIRST_NAME',   type:'varchar', version:1, deleted:0, semantic:'person_name',         sem_source:'rule', validated:1 },
            { name:'LAST_NAME',    type:'varchar', version:2, deleted:0, semantic:'person_name',         sem_source:'rule', validated:1 },
            { name:'EMAIL',        type:'varchar', version:1, deleted:0, semantic:'email',               sem_source:'rule', validated:1 },
            { name:'PHONE',        type:'varchar', version:1, deleted:0, semantic:'phone_number',        sem_source:'rule', validated:0 },
            { name:'CREDIT_LIMIT', type:'decimal', version:3, deleted:0, semantic:'financial_value',     sem_source:'llm',  validated:0 },
            { name:'OLD_SEGMENT',  type:'varchar', version:2, deleted:1, semantic:'',                    sem_source:'',     validated:0 },
            ]
        },
        'ORDERS': {
            columns: [
            { name:'ORDER_ID',     type:'integer', version:1, deleted:0, semantic:'identifier',          sem_source:'rule', validated:1 },
            { name:'CUSTOMER_ID',  type:'integer', version:1, deleted:0, semantic:'customer_identifier', sem_source:'rule', validated:1 },
            { name:'ORDER_DATE',   type:'date',    version:1, deleted:0, semantic:'date',                sem_source:'rule', validated:1 },
            { name:'TOTAL_AMOUNT', type:'decimal', version:2, deleted:0, semantic:'financial_value',     sem_source:'llm',  validated:0 },
            { name:'STATUS',       type:'varchar', version:1, deleted:0, semantic:'status',              sem_source:'rule', validated:1 },
            ]
        }
        }
    },
    p2: {
        name: '0_testing_mssql_to_psql',
        tables: {
        'human_resources_shift': {
            columns: [
            { name:'shift_id',   type:'integer', version:1, deleted:0, semantic:'identifier', sem_source:'rule', validated:1 },
            { name:'name',       type:'varchar', version:1, deleted:0, semantic:'name',       sem_source:'rule', validated:1 },
            { name:'start_time', type:'time',    version:1, deleted:0, semantic:'time',       sem_source:'llm',  validated:0 },
            { name:'end_time',   type:'time',    version:1, deleted:0, semantic:'time',       sem_source:'llm',  validated:0 },
            ]
        }
        }
    },
    p3: {
        name: 'testing_transform_pgsql',
        tables: {
        'people_data': {
            columns: [
            { name:'shift_id',   type:'integer', version:1, deleted:0, semantic:'identifier', sem_source:'rule', validated:1 },
            { name:'name',       type:'varchar', version:1, deleted:0, semantic:'name',       sem_source:'rule', validated:1 },
            { name:'start_time', type:'time',    version:1, deleted:0, semantic:'time',       sem_source:'llm',  validated:0 },
            { name:'end_time',   type:'time',    version:1, deleted:0, semantic:'time',       sem_source:'llm',  validated:0 },
            ]
        }
        }
    }
    },


    RULES: [
    { id:1,  pattern:'.*_id$|^id$',                                           concept:'identifier',          confidence:0.95 },
    { id:2,  pattern:'.*_at$|.*_date$|^date.*|.*date$',                       concept:'date',                confidence:0.95 },
    { id:3,  pattern:'.*name.*',                                              concept:'name',                confidence:0.90 },
    { id:4,  pattern:'.*amount.*|.*price.*|.*salary.*|.*revenue.*|.*total.*', concept:'financial_value',     confidence:0.90 },
    { id:5,  pattern:'.*email.*',                                             concept:'email',               confidence:0.98 },
    { id:6,  pattern:'.*phone.*|.*mobile.*',                                  concept:'phone_number',        confidence:0.95 },
    { id:7,  pattern:'.*address.*|.*street.*|.*city.*|.*zip.*',               concept:'address',             confidence:0.88 },
    { id:8,  pattern:'.*status.*|.*state.*',                                  concept:'status',              confidence:0.85 },
    { id:9,  pattern:'.*count.*|.*qty.*|.*quantity.*',                        concept:'metric',              confidence:0.85 },
    { id:10, pattern:'.*customer.*|.*client.*|.*cust.*',                      concept:'customer_identifier', confidence:0.92 },
    ]

}