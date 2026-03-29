/**
 * Utility functions for handling different destination types
 */

import { PipelineService } from "./PipelineService.js";


const CLOUD_WAREHOUSE_DEST = {
    DATABRICKS: 'databricks',
    BIG_QUERY: 'bigquery',
}

const SQL_DB = {
    PGSQL: 'postgresql',
    MSSQL: 'mssql',
    ORCLE: 'oracle',
    MYSQL: 'mysql'
}

const DEST_TYPE = {
    SQL: 'sql',
    DUCKDB: 'duckdb',
    ...SQL_DB,
    ...CLOUD_WAREHOUSE_DEST
}

export const nonDuckDBSupport = Object.values({ ...CLOUD_WAREHOUSE_DEST, ...SQL_DB, SQL: 'sql' });

/**
 * Check if destination type is a non-DuckDB destination (SQL, BigQuery, Databricks)
 * @param {string} destType - The destination type
 * @returns {boolean} True if destination is SQL, BigQuery, or Databricks
 */
export function isNonDuckDBDestination(destType) {
    return Object.values(DEST_TYPE).includes(destType);
}

/**
 * Construct table path based on destination type
 * 
 * Different database systems use different identifier formats:
 * - SQL databases (PostgreSQL, MySQL, SQL Server, Oracle, MariaDB): schema.table (2-part)
 * - DuckDB: database.schema.table (3-part)
 * - BigQuery: project.dataset.table (3-part, different semantics)
 * 
 * @param {Object} tableDetail - Table detail object containing dest, dbname, and table
 * @param {string} database - Database name (used for DuckDB)
 * @returns {string} Constructed table path
 */
export function constructTablePath(tableDetail, database = null, pipelineName) {
    if (isNonDuckDBDestination(tableDetail.dest)) {
        // For SQL, BigQuery, and Databricks: use schema.table format (two-part)
        // Defensive: if dbname contains a dot, extract only the schema part
        // This handles cases where metadata incorrectly includes "database.schema" format
        let schema = tableDetail.dbname;
        if (schema && schema.includes('.')) {
            // Extract schema from "database.schema" format
            const parts = schema.split('.');
            schema = parts[parts.length - 1]; // Take the last part (schema)
        }
        return `${pipelineName}.${schema}.${tableDetail.table}`;
    } else {
        // For DuckDB: use database.dbname.table format (three-part)
        return `${database}.${tableDetail.dbname}.${tableDetail.table}`;
    }
}

/**
 * Get supported destination types for query functionality
 * @returns {Array<string>} Array of supported destination types
 */
export function getSupportedQueryDestinations() {
    return [
        DEST_TYPE.DUCKDB, 
        DEST_TYPE.SQL, 
        DEST_TYPE.BIG_QUERY, 
        DEST_TYPE.DATABRICKS,
        DEST_TYPE.MSSQL,
        DEST_TYPE.MYSQL,
        DEST_TYPE.PGSQL,
        DEST_TYPE.ORCLE,
    ];
}

/**
 * Adjust SQL query for database-specific syntax
 * Handles differences in LIMIT clauses and other SQL dialect variations
 * 
 * @param {string} query - The SQL query to adjust
 * @param {string} dbEngine - Database engine ('mysql', 'postgresql', 'mssql', 'oracle', 'unknown')
 * @returns {string} Adjusted SQL query
 */
export function adjustQueryForDialect(query, dbEngine) {
    if (!query || !dbEngine) {
        return query;
    }
    
    const engine = dbEngine.toLowerCase();
    
    // Handle LIMIT clause differences
    if (engine === 'mssql') {
        // SQL Server uses TOP instead of LIMIT
        // Convert: SELECT * FROM table LIMIT 100
        // To: SELECT TOP 100 * FROM table
        const limitMatch = query.match(/\s+LIMIT\s+(\d+)/i);
        if (limitMatch) {
            const limitValue = limitMatch[1];
            // Remove LIMIT clause
            query = query.replace(/\s+LIMIT\s+\d+/i, '');
            // Add TOP clause after SELECT
            query = query.replace(/SELECT\s+/i, `SELECT TOP ${limitValue} `);
        }
    } else if (engine === DEST_TYPE.ORCLE) {
        // Oracle uses FETCH FIRST n ROWS ONLY (Oracle 12c+) or ROWNUM (older versions)
        // Convert: SELECT * FROM table LIMIT 100
        // To: SELECT * FROM table FETCH FIRST 100 ROWS ONLY
        const limitMatch = query.match(/\s+LIMIT\s+(\d+)/i);
        if (limitMatch) {
            const limitValue = limitMatch[1];
            // Remove LIMIT clause and add FETCH FIRST
            query = query.replace(/\s+LIMIT\s+\d+/i, ` FETCH FIRST ${limitValue} ROWS ONLY`);
        }
    }
    // MySQL and PostgreSQL both support LIMIT, so no changes needed
    
    return query;
}

/**
 * Generate initial SQL query with proper syntax for the database engine
 * 
 * @param {string} tableName - Full table name (schema.table for SQL databases)
 * @param {Array<string>} fields - Array of field names
 * @param {string} dbEngine - Database engine type
 * @param {number} limit - Number of rows to limit (default: 100)
 * @returns {string} Generated SQL query
 */
export function generateInitialQuery(tableName, fields, sourceDestDetails, limit = 100) {
    
    let dbEngine, sourceType, isSourceSQL;
    if('destType' in (sourceDestDetails || {})){
        // By doing .match it extracts the dbengine type
        dbEngine = sourceDestDetails.destType.match(/(SQL_DEST|CODE_DEST)\(([^)]+)\)/);
        sourceType = sourceDestDetails.sourceType.match(/(SQL_SOURCE|CODE_SOURCE)\(([^)]+)\)/);
        isSourceSQL = sourceDestDetails.sourceType.startsWith('SQL_SOURCE');

        if(sourceType) sourceType = sourceType[2];
		if(dbEngine) dbEngine = dbEngine[2];
    }else
        dbEngine = SQL_DB.MYSQL;
        
    
    const isPostGresOrSqlServer = [SQL_DB.PGSQL, SQL_DB.MSSQL].includes(dbEngine);
    
    const fieldsString = fields && fields.length > 0 
        ? fields.map(f => (isPostGresOrSqlServer || f.trim().indexOf(' ') > 0) ? `"${f}"` : f).join(', ') 
        : '*';

    tableName = isPostGresOrSqlServer ? tableName.split('.').map(t => `"${t}"`).join('.') : tableName;
    const engine = (dbEngine || SQL_DB.MYSQL).toLowerCase();

    if(Object.values(SQL_DB).includes(dbEngine) && sourceDestDetails !== undefined) {
        PipelineService.sqlEditorDestType = DEST_TYPE.SQL;
        PipelineService.sqlEditorDestSecretName = sourceDestDetails.destSecretName;
    }else{
        PipelineService.sqlEditorDestType = engine;
    }

    if((sourceDestDetails || {}).referencedSecrets)
        PipelineService.pipelineReferencedSecrets = JSON.parse(sourceDestDetails.referencedSecrets.replace(/'/g, '"'));

    if((sourceDestDetails || {}).destinationConfig)
        PipelineService.pipelineDestinationConfig = sourceDestDetails.destinationConfig;

    if((sourceDestDetails || {}).datasetName)
        PipelineService.pipelineDestinationDB = sourceDestDetails.datasetName;
    
    if (engine === DEST_TYPE.MSSQL) {
        // SQL Server: SELECT TOP n
        return `SELECT TOP ${limit} ${fieldsString}\nFROM ${tableName}`;

    } else if (engine === DEST_TYPE.ORCLE) {

        // Oracle: FETCH FIRST n ROWS ONLY
        return `SELECT ${fieldsString}\nFROM ${tableName}\nFETCH FIRST ${limit} ROWS ONLY`;

    } else {
        
        const isPgsqlDestination = engine == DEST_TYPE.PGSQL;

        let table = tableName.startsWith('.') ? tableName.slice(1) : tableName;

        if(sourceDestDetails?.destType === 'DUCKDB_DEST'){
            table = tableName.split('.').slice(1).join('.');
        }else{
            if(isSourceSQL && sourceType !== DEST_TYPE.MSSQL) 
                table = table.split('.').slice(-1);

            if(sourceType === DEST_TYPE.MSSQL){
                const database = table.split('.')[0];
                let tablePath = table.split('.').slice(1).join('_').replaceAll('"','');

                if(isPgsqlDestination) 
                    tablePath = `"${tablePath}"`;

                table = `${database}.${tablePath}`;
            }
        }
        
        return `SELECT ${fieldsString}\nFROM ${table}\nLIMIT ${limit}`;
    }
}