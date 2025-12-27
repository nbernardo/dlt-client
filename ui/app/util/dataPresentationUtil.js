export const dataToTable = (data, title = null, isArrayData = false) => {

    let fields = [], header, rows = [], tableBody = '', addClass = '';
    const headerStyle = 'style="background: #808080b5; color: white; font-weight: bold;"';
    
    if(isArrayData){
        rows = data.data, fields = data.columns, addClass = data.cssClass;
    }else{
        rows = Object.values(Object.values(data));
        fields = Object.keys(rows[0]);
    }

    header = `${fields.map(field => `<td>${field}</td>`).join('')}`;        
    title = title != null ? `<tr ${headerStyle}><td colspan="${fields.length}">${title}</td></tr>` : '';

    if(isArrayData){
        for (const row of rows)
            tableBody += `<tr>${row.map(colData => `<td>${colData}</td>`).join('')}</tr>`;
    }else{
        for (const row of rows)
            tableBody += `<tr>${fields.map(field => `<td>${row[field]}</td>`).join('')}</tr>`;
    }


    return `<table class="${addClass}">
                <thead>
                    ${title}
                    <tr ${headerStyle}>${header}</tr>
                </thead>
                <tbody>${tableBody}</tbody>
            </table>
            `;
}