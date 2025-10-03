export class AIAgentUtil {

    /** 
     * @param {string} fields
     * @param { Array<Array> } data
      */
    parseDataToTable(fields, data){

        const fieldsHeader = 
            '<tr>'+fields.split(',')
                .map(field => `<td class="right">${field.trim()}</td>`).join('').replaceAll('\'','')
            +'</tr>';

        const tableBody = data.map(row => 
            `<tr>${row.map(fieldVal => `<td class="right">${fieldVal}</td>`).join('')}</tr>`
        ).join('');

        return `<b>Scoll to right if more content is hidden</b><br>
                <table class="chatbot-response-table">
                    <thead>${fieldsHeader}</thead>
                    <tbody>${tableBody}</tbody>
                </table>`;

    }

}