import { UUIDUtil } from "../../@still/util/UUIDUtil.js";

class InputDropdownParam {
    inputSelector; //The selector (e.g. css className) of the html input which is being bound
    filterableListSelector;//INTERNALLY HANDLED:  The selector (e.g. css className) of the html list (<ul>) to list the filtered results
    dataSource; //Array with the list of the elements to be filtered, assigned/passed from InputDropdown instance 
    boundComponent; //The component where the field is placed, normally recieves this
    componentFieldName; //INTERNALLY HANDLED: field name/state variable of the component which the dropdown is being bound, 

    /**  
     * @param { any } value
     * @param { InputDropdown } self
     */
    onSelect(value, self) {};
}

export class InputDropdown {

    dataSource = [];

    filterInput;
    filterableList;
    listItems;
    /** @type { Array<InputDropdown> } */
    relatedFields = [];
    componentBoundField;
    componentFieldName;

    #params;

    /** @param { InputDropdownParam } params  */
    static new(params){

        const resultListId = 'dynamicFilter-'+UUIDUtil.newId();
        const filterResultLst = `<ul id="${resultListId}" class="filterable-list-dropdown hidden"></ul>`;
        const /** @type { HTMLInputElement } */ inputHTMLElement = document.querySelector(params.inputSelector);
        inputHTMLElement.insertAdjacentHTML('afterend',filterResultLst);
        params.filterableListSelector = `#${resultListId}`;
        params.componentFieldName = inputHTMLElement.dataset.stFieldName;
        
        return new InputDropdown(params);
        
    }

    /** @param { InputDropdownParam } params  */
    constructor(params) {
        
        this.#params = params;
        this.componentFieldName = params.componentFieldName;
        if (params.dataSource) this.dataSource = params.dataSource;
        if (params.onSelect){
            this.onSelect = async (selectedVal) => {
                await params.onSelect(selectedVal, this);
            }
        }

        this.filterInput = document.querySelector(params.inputSelector);
        this.filterableList = document.querySelector(params.filterableListSelector);
        this.populateList();
        this.initInputHandling();
    }

    populateList() {
        
        const self = this;
        this.filterableList.innerHTML = '';
        const params = this.#params;

        this.dataSource.forEach((fruit) => {
            const li = document.createElement('li');
            li.onclick = () => {
                if(params.boundComponent){
                    params.boundComponent[params.componentFieldName] = li.innerText;
                }
                self.onSelect(li.innerText);
            }
            li.textContent = fruit;
            li.classList.add('list-item-dropdown');
            self.filterableList.appendChild(li);
        });

        this.listItems = this.filterableList.getElementsByTagName('li');
    }

    setDataSource(dataSource){
        this.dataSource = dataSource;
        this.populateList();
    }

    filterList(event) {
        
        const filterText = this.filterInput.value.toLowerCase().trim();
        let matchFound = false, showAll = false;
        if(event?.key === 'Control') showAll = true;

        for (let i = 0; i < this.listItems.length; i++) {
            const item = this.listItems[i];
            const itemText = item.textContent || item.innerText;
            if(showAll) item.classList.remove('hidden');
            else {
                if (itemText.toLowerCase().includes(filterText)) {
                    item.classList.remove('hidden');
                    matchFound = true;
                }
                else item.classList.add('hidden');
            }
        }

        if ((filterText.length > 0 && matchFound) || (showAll && this.listItems.length > 0))
            this.filterableList.classList.remove('hidden');
        else
            this.filterableList.classList.add('hidden');
    }

    initInputHandling() {
        const self = this;
        this.filterInput.addEventListener('input', (event) => self.filterList(event));
        this.filterInput.addEventListener('keyup', (event) => self.filterList(event));

        this.filterableList.addEventListener('click', (event) => {
            if (event.target.tagName === 'LI' && !event.target.classList.contains('hidden')) {
                self.filterInput.value = event.target.textContent;
                self.filterableList.classList.add('hidden');
                self.filterInput.focus();
            }
        });

        this.filterInput.addEventListener('blur', () => {
            setTimeout(() => self.filterableList.classList.add('hidden'), 150);
        });
    }

    onSelect = async (value, self) => {};

}

