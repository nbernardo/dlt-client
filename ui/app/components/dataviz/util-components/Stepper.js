import { BaseComponent } from "../../../../@still/component/super/BaseComponent.js";
import { UUIDUtil } from "../../../../@still/util/UUIDUtil.js";
import { StillAppSetup } from "../../../../config/app-setup.js";

class StepperOptions{ 
    start;  end;  step;  unit; label; isDate; onColumnSelect; 
    /** @type { BaseComponent } */ 
    component;
}

export class Stepper {

    id = '_'+UUIDUtil.newId();
    static fieldListSource = {};
    static methodNames = {};
    static components = {};

    constructor(){
        // This is done so this class/util is made available globally
        StillAppSetup.register(Stepper);
    }

    /** @returns { Array } */
    static getFieldList = (id) => Stepper.fieldListSource[id];
    static setFieldList = (id, values) => Stepper.fieldListSource[id] = values;

    static getOnFieldSelection = (id) => Stepper.methodNames[id];
    static setOnFieldSelection = (id, methodName) => Stepper.methodNames[id] = methodName;

    /** @returns { BaseComponent } */
    static getComponent = (id) => Stepper.components[id];
    static setComponent = (id, component) => Stepper.components[id] = component;

    /** @returns { Stepper } */
    static new(container, /** @type { StepperOptions } */ options = {}){
        const stepper = new Stepper()
        stepper.initStepper(container, options);
        Stepper.setOnFieldSelection(stepper.id, options.onColumnSelect);
        Stepper.setComponent(stepper.id, options.component);
        return stepper;
    }

    initStepper(container, /** @type { StepperOptions } */ options = {}) {
      
        const isDate = options.isDate;
        container.className = 'stepper-range-with-slider';
        container.innerHTML = this.stepperBody((isDate ? 'date' : 'number'), (options.label || 'Range'));
        const rMin = container.querySelector('.min'), rMax = container.querySelector('.max');
        const iMin = container.querySelector('.in-min'), iMax = container.querySelector('.in-max');
        const wrapper = container.querySelector('.wrapper');

        const start = isDate ? new Date(options.start).getTime() : options.start;
        const end = isDate ? new Date(options.end).getTime() : options.end;
        const step = isDate ? (options.step || 1) * 86400000 : (options.step || 1);

        [rMin, rMax].forEach(r => { r.min = start; r.max = end; r.step = step; });
        rMin.value = start; rMax.value = end;

        const update = (e) => {
            let v1 = parseInt(rMin.value), v2 = parseInt(rMax.value);

            if (e?.target.classList.contains('range-input')) {
              if (v1 > v2) {
                if (e.target === rMin) rMin.value = v2; else rMax.value = v1;
                v1 = parseInt(rMin.value); v2 = parseInt(rMax.value);
              }
            }

            if (isDate) {
              iMin.value = new Date(v1).toISOString().split('T')[0];
              iMax.value = new Date(v2).toISOString().split('T')[0];
            } else {
                iMin.value = v1; iMax.value = v2;
            }

          const p1 = (v1 - start) / (end - start) * 100;
          const p2 = (v2 - start) / (end - start) * 100;
          wrapper.style.background = `linear-gradient(to right, #ddd ${p1}%, #5d93e1 ${p1}%, #5d93e1 ${p2}%, #ddd ${p2}%)`;
        };

        rMin.oninput = rMax.oninput = update;

        [iMin, iMax].forEach(input => {
          input.onchange = () => {
            rMin.value = isDate ? new Date(iMin.value).getTime() : iMin.value;
            rMax.value = isDate ? new Date(iMax.value).getTime() : iMax.value;
            update();
          };
        });

        update();
    }

    stepperBody(type = 'number', label = "Range Selector"){
        // Because Stepper class is not a Still component nor a controller, the call of any of its 
        // methods (e.g. updateFieldList) on the onclick event is being implemented as static 
        return `
          <div class="stepper-top-controls">
            <div class="stepper-data">
                <div class="input-box">
                    <label class="main-label">${label}:</label> <input type="${type}" class="in-min"> <input type="${type}" class="in-max">
                </div>
            </div>
            <div class="datasource">
                <img src="/app/assets/imgs/database_.png" width="12" onclick="Stepper.openSourceOptions('${this.id}')">
                <div class="list-of-fields ${this.id}" style="display: none;">
                    <select class="tables-${this.id}" onchange="Stepper.updateFieldList(this.value, '${this.id}')"></select>
                    <div class="available-fields available-fields-${this.id}"></div>
                </div>
            </div>
          </div>
          <div class="wrapper"> <input type="range" class="range-input min"> <input type="range" class="range-input max"> </div>
        `;
    }

    updateTablesList(tablesList = []){
        Stepper.fieldListSource = {};
        Stepper.setFieldList(this.id, tablesList);
        document.querySelector(`.tables-${this.id}`).innerHTML = tablesList.map(tbl => `<option value="${tbl.name}">${tbl.name}</option>`).join('');
    }

    static updateFieldList(table, id){
        const columnList = Stepper.getFieldList(id).find(it => it.name == table)?.cols || [];

        // This is the event name which can be from the 
        // component of from the controller
        const eventName = Stepper.getOnFieldSelection(id);

        // Because the event will come from the component of its controller, 
        // the component itself need to parse the events
        const component = Stepper.getComponent(id);

        document.querySelector(`.available-fields-${id}`).innerHTML = `
            <div class="fields-panel-body">
                ${columnList.map(f=>
                    component.parseEvents(`<div class="field-item">
                        <input type="radio" onclick="${eventName}('${f.name}','${f.column_name}')">${f.column_name}
                        <span class="field-type">${f.data_type}</span>
                    </div>`)
                ).join('')}
            </div>
        `;
    }

    static openSourceOptions(id){
        const fieldsContainer = document.querySelector(`.${id}`);
        const display = fieldsContainer.style.display === 'none' ? '' : 'none';
        fieldsContainer.style.display = display;
    }
}