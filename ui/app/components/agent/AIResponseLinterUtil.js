export class AIResponseLinterUtil {

    static formatSQL(sql) {

        sql = sql.trim().replace(/\s+/g, ' ');

        let formatted = sql;
        formatted = formatted.replace(/\b(FROM|WHERE|GROUP BY|HAVING|ORDER BY|LIMIT|OFFSET)\b/gi, '\n$1');
        formatted = formatted.replace(/\b(INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN|CROSS JOIN|JOIN)\b/gi, '\n$1');
        formatted = formatted.replace(/\bON\b/gi, '\n  ON');

        formatted = formatted.replace(/SELECT\s+(.+?)(?=\s+FROM)/gi, (match, columns) => {
            const columnList = columns.split(',').map(col => col.trim());
            if (columnList.length > 1) {
                const lines = [];
                for (let i = 0; i < columnList.length; i += 4) {
                    lines.push(columnList.slice(i, i + 4).join(', '));
                }
                return 'SELECT\n  ' + lines.join(',\n  ');
            }
            return 'SELECT ' + columns;
        });

        const lines = formatted.split('\n').map(line => line.trim());
        const indented = lines.map((line, index) => {
            if (index === 0) return line;

            if (line.match(/^[a-z_]/i) && !line.match(/^(FROM|WHERE|GROUP|HAVING|ORDER|LIMIT|OFFSET)/i)) {
                return line;
            }
            return line;
        });

        return indented.join('\n');
    }

}