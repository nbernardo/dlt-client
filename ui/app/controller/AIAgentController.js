import { AIResponseLinterUtil } from "../components/agent/AIResponseLinterUtil.js";
import { Workspace } from "../components/workspace/Workspace.js";

export class AIAgentController {

    /** 
     * @param {string} fields
     * @param { Array<Array> } data
     * @param { Workspace } workspaceComponent 
      */
    parseDataToTable(fields, data, workspaceComponent, actualQuery) {

        const fieldsHeader =
            '<tr>' + fields.split(',')
                .map(field => `<th class="right">${field.trim()}</th>`).join('').replaceAll('\'', '')
            + '</tr>';

        const tableBody = data.map(row =>
            `<tr>${row.map(fieldVal => `<td class="right">${fieldVal}</td>`).join('')}</tr>`
        ).join('');

        const scrolingLbl = `<b style="display: block;">Scoll to right if more content is hidden</b>`;

        // Workspace component will parse the event, then inner.expandDataTableView() points to
        // a method that is implemented within itself, hence inner
        let expandViewLink = `<a href="#" onclick="inner.expandDataTableView()">Expand View</a>`;
        expandViewLink = workspaceComponent.parseEvents(expandViewLink);

        const actualTable = `${scrolingLbl}<br>${expandViewLink}
                <table class="chatbot-response-table">
                    <thead>${fieldsHeader}</thead>
                    <tbody>${tableBody}</tbody>
                </table>`;

        // Passing data to the controller so it can be accessed by the Workspace component
        workspaceComponent.controller.aiAgentExpandView.data = data;
        workspaceComponent.controller.aiAgentExpandView.fields = fields;
        setTimeout(() => 
            workspaceComponent.controller.aiAgentExpandView.query = AIResponseLinterUtil.formatSQL(actualQuery),
        0);
        workspaceComponent.controller.aiAgentExpandView.initialTable = actualTable;

        return actualTable;

    }

}