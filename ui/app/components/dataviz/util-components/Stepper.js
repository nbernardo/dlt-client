import { BaseComponent } from "../../../../@still/component/super/BaseComponent.js";
import { UUIDUtil } from "../../../../@still/util/UUIDUtil.js";
import { StillAppSetup } from "../../../../config/app-setup.js";

class StepperOptions{ 
    start;  end;  step;  unit; label; isDate; onColumnSelect; onRangeChange; 
    /** @type { BaseComponent } */ 
    component;
}

export class Stepper {

    id = '_'+UUIDUtil.newId();
    label;
    start; end; rMin; rMax; iMin; iMax;

    static fieldListSource = {};
    static methodNames = {};
    static onRangeChange = {};
    static components = {};
    static innerOnSelectEvents = {};
    static onTableChange = {};
    static appPath = '';
    /** @type { StepperOptions } */ options;
    /** @type { HTMLElement } */ container;

    constructor(){
        // This is done so this class/util is made available globally
        StillAppSetup.register(Stepper);
    }

    /** @returns { Array } */
    static getFieldList = (id) => Stepper.fieldListSource[id];
    static setFieldList = (id, values) => Stepper.fieldListSource[id] = values;

    static getOnFieldSelection = (id) => Stepper.methodNames[id];
    static setOnFieldSelection = (id, methodName) => Stepper.methodNames[id] = methodName;

    static getOnRangeChange = (id) => Stepper.onRangeChange[id];
    static setOnRangeChange = (id, methodName) => Stepper.onRangeChange[id] = methodName;

    static getOnTableChange = (id) => Stepper.onTableChange[id];
    static setOnTableChange = (id, methodName) => Stepper.onTableChange[id] = methodName;

    /** @returns { Stepper } */
    static getComponent = (id) => Stepper.components[id];
    static setComponent = (id, component) => Stepper.components[id] = component;

    /** @returns { Stepper } */
    static new(container, /** @type { StepperOptions } */ options = {}){
        const stepper = new Stepper()
        stepper.initStepper(container, options);

        if(options.onColumnSelect)
            Stepper.setOnFieldSelection(stepper.id, options.onColumnSelect);

        Stepper.setComponent(stepper.id, stepper);
        return stepper;
    }

    onRangeChange(cb = ({ max, min, field }) => {}){ Stepper.onRangeChange[this.id] = cb; }

    onColumnSelect(cb = (column) => {}){ Stepper.setOnFieldSelection(this.id, cb); }

    onDataRangeSelect(cb = () => {}){ Stepper.setOnTableChange(this.id, cb); }

    initStepper(containerElm, /** @type { StepperOptions } */ elmOptions = {}) {
        if(containerElm){
            this.container = containerElm, this.options = elmOptions;
        }else{            
            this.options.start = elmOptions.start, this.options.end = elmOptions.end, this.options.isDate = elmOptions.isDate;
            if(elmOptions.label) 
                this.container.querySelector(`.current-stepper-label-${this.id}`).innerHTML = elmOptions.label;

            const type = elmOptions.isDate ? 'date' : 'number';
            this.iMin.type = type, this.iMax.type = type;

            this.rMax.min = Number(elmOptions.start), this.rMax.max = Number(elmOptions.end);
            this.rMin.min = Number(elmOptions.start), this.rMin.max = Number(elmOptions.end);
        }

        const { container, options } = this;
        const isDate = options.isDate;
        
        if(containerElm){
            container.className = 'stepper-range-with-slider';
            container.innerHTML = this.stepperBody((isDate ? 'date' : 'number'), (options.label || 'Range'));
            this.rMin = container.querySelector('.min'), this.rMax = container.querySelector('.max');
            this.iMin = container.querySelector('.in-min'), this.iMax = container.querySelector('.in-max');
        }
        
        const { rMax, rMin, iMax, iMin } = this;

        if(!elmOptions.end || !elmOptions.start){
            iMax.disabled = true, iMin.disabled = true;
            iMax.value = '', iMin.value = '';
            return;
        }
        
        iMax.disabled = false, iMin.disabled = false;

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
                if (e.target === rMin) rMin.value = v2; 
                else rMax.value = v1;
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
            if(Stepper.onRangeChange[this.id])
                Stepper.onRangeChange[this.id]({ max: iMax.value, min: iMin.value, field: this.totalRecords > 0 ? 'Data range' : elmOptions.label });

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
                    <label class="main-label current-stepper-label-${this.id}">${label}:</label>
                    <div style="display: flex; gap: 4px;"><input type="${type}" class="in-min"> <input type="${type}" class="in-max"></div>
                </div>
            </div>
            <div class="datasource">
                <img src="${Stepper.appPath}/app/assets/imgs/database_.png" width="12" onclick="Stepper.openSourceOptions('${this.id}')">
                <div class="list-of-fields ${this.id}" style="display: none;">
                    <select class="tables-${this.id}" onchange="Stepper.updateFieldList(this.value, '${this.id}')">
                        <option value="">No model selected</option>
                    </select>
                    <div class="available-fields available-fields-${this.id}"></div>
                </div>
            </div>
          </div>
          <div class="wrapper"> <input type="range" class="range-input min"> <input type="range" class="range-input max"> </div>
        `;
    }

    updateTablesList(tablesList = []){
        Stepper.fieldListSource = {}, tablesList = tablesList.length ? [{ name: 'Select a table' }, { name: 'Data range' }, ...tablesList] : [{ name: 'No model selected' }];
        Stepper.setFieldList(this.id, tablesList);
        Stepper.setSelectRangeField(this.id);
        document.querySelector(`.tables-${this.id}`).innerHTML = tablesList.map(tbl => `<option value="${tbl.name}">${tbl.name}</option>`).join('');
    }

    totalRecords = 0;
    static updateFieldList(table, id){
        
        const component = Stepper.getComponent(id);
        component.totalRecords = 0;
        const columnList = Stepper.getFieldList(id).find(it => it.name == table)?.cols || [];
        const evt = Stepper.getOnTableChange(id);

        if(table === 'Data range'){
            if(evt) evt(table);
            component.initStepper(null, { start: 1, end: component.totalRecords, label: table, isDate: false });
        }

        document.querySelector(`.available-fields-${id}`).innerHTML = `
            <div class="fields-panel-body">
                ${columnList.filter(f => ['datetime','date','timestamp','time'].includes(f.data_type)).map(f=>
                    `<div class="field-item">
                        <input type="radio" name="column" onclick="Stepper.runSelectRangeField('${id}','${table}_${f.column_name}')">${f.column_name}
                        <span class="field-type">${f.data_type}</span>
                    </div>`
                ).join('')}
            </div>
        `;
    }

    setRangeValues({ start, end }){
        this.start = start, this.end = end;
    }

    static setSelectRangeField(id){
        
        Stepper.innerOnSelectEvents[id] = (table, value) => {
            // This is the event name which can be from the 
            // component of from the controller
            const eventName = Stepper.getOnFieldSelection(id);
            const component = Stepper.getComponent(id);
            if(eventName) eventName(table, value);

            component.initStepper(null, { start: component?.start, end: component?.end, label: component.label, isDate: true });
        }

    }

    static runSelectRangeField = (id, fieldPath) => Stepper.innerOnSelectEvents[id](fieldPath);

    static openSourceOptions(id){
        const fieldsContainer = document.querySelector(`.${id}`);
        const display = fieldsContainer.style.display === 'none' ? '' : 'none';
        fieldsContainer.style.display = display;
    }
}