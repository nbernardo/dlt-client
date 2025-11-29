import { $still } from "../../../../@still/component/manager/registror.js";

const baseUrl = `${window.location.protocol}//${window.location.host}`;


export async function loadTemplate(tmplt_name){
    const tmplt = await $still.HTTPClient.get(`${baseUrl}/app/assets/dlt-code-template/${tmplt_name}`);
    return await tmplt.text();  
}