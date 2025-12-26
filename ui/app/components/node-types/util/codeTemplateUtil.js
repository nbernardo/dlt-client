import { $still } from "../../../../@still/component/manager/registror.js";

const baseUrl = `${window.location.protocol}//${window.location.host}`;


export async function loadTemplate(tmplt_name, type = 'source'){
    let tmplt;
    if(type == 'source')
        tmplt = await $still.HTTPClient.get(`${baseUrl}/app/assets/dlt-code-template/source/${tmplt_name}`);
    else if(type == 'dest')
        tmplt = await $still.HTTPClient.get(`${baseUrl}/app/assets/dlt-code-template/destination/${tmplt_name}`);
    return await tmplt.text();  
}