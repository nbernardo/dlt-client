import { BehaviorComponent } from "../component/super/BehaviorComponent.js";
class InParams {className; id; datasets = {}; type; placeholder; min; max; required;} ;
export const FormHelper = {
    newField(cmp, formRef, fieldName, value = null){
        //Components is available globally from import { Components } from "../setup/components";
        Components.ref(cmp.cmpInternalId)[fieldName] = value
        Components.obj().parseGetsAndSets(cmp, fieldName);
        return {
            /** @param { InParams } params  */
            getInput(params = inParams){
                const {className, id, datasets = {}, type, placeholder, min, max, required} = params;
                const datafields = Object.entries(datasets).map(([f,v]) => (`data-${f}="${v}"`)).join(' ');
                const ftype=`type="${type || 'text'}"`;
                const hint = `${placeholder ? `placeholder="${placeholder}"` : ''}`;
                const val = `${value ? `value="${value}"` : ''}`, _id = `${id ? `id="${id}"` : ''}`;
                const mn = `${min ? `min="${min}"` : ''}`, mx = `${max ? `max="${max}"` : ''}`;
                const req = `${required ? ' (required)="true" ' : ''}`;
                const validatorClass = BehaviorComponent.setOnValueInput(req, cmp, fieldName, (formRef.name || null));
                const input = `
                    <input ${datafields}
                        class="${validatorClass} listenChangeOn-${cmp.cmpInternalId}-${fieldName} ${cmp.cmpInternalId}-${fieldName} ${className || ''}"
                        ${ftype} ${val} ${_id} ${req.trim()} ${hint} ${mn} ${mx}
                    >
                `;
                return {
                    add(cb = function(input){}, subContainer = null){
                        const cnt = cb(input);
                        const ctr = document.getElementById(formRef.formId);
                        ctr.insertAdjacentHTML('beforeend', cnt || input);
                    },
                    element: input 
                }
            }
        }
    },

}