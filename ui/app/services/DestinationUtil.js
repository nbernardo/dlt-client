/**
 * Utility functions for handling different destination types
 */

/**
 * Check if destination type is a non-DuckDB destination (SQL, BigQuery, Databricks)
 * @param {string} destType - The destination type
 * @returns {boolean} True if destination is SQL, BigQuery, or Databricks
 */
export function isNonDuckDBDestination(destType) {
    return destType === 'sql' || destType === 'bigquery' || destType === 'databricks';
}

/**
 * Construct table path based on destination type
 * @param {Object} tableDetail - Table detail object containing dest, dbname, and table
 * @param {string} database - Database name (used for DuckDB)
 * @returns {string} Constructed table path
 */
export function constructTablePath(tableDetail, database = null) {
    if (isNonDuckDBDestination(tableDetail.dest)) {
        // For SQL, BigQuery, and Databricks destinations: use schema.table format
        return `${tableDetail.dbname}.${tableDetail.table}`;
    } else {
        // For DuckDB: use database.dbname.table format
        return `${database}.${tableDetail.dbname}.${tableDetail.table}`;
    }
}

/**
 * Get supported destination types for query functionality
 * @returns {Array<string>} Array of supported destination types
 */
export function getSupportedQueryDestinations() {
    return ['duckdb', 'sql', 'bigquery', 'databricks'];
}
