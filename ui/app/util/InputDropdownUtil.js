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
    /**  
     * @param { any } value
     * @param { InputDropdown } self
     */
    onLoseFocus(value, self) {};
}

const invalidClass = 'stjs-int-fltr-invld-val';
export class InputDropdown {

    dataSource = [];
    highlightedIndex = -1;

    filterInput;
    filterableList;
    listItems;
    /** @type { Array<InputDropdown> } */
    relatedFields = [];
    componentBoundField;
    componentFieldName;

    #params;

    static #stylesInjected = false;
    static injectStyles() {
        if (InputDropdown.#stylesInjected) return;
        InputDropdown.#stylesInjected = true;
        const style = document.createElement('style');
        style.id = 'input-dropdown-util-styles';
        style.textContent = `.list-item-dropdown.highlighted { background: #e6f0ff; }\n .${invalidClass}{ border: 2px solid #ff00006e !important; background: #ff00001a !important; }`;
        document.head.appendChild(style);
    }

    /** @param { InputDropdownParam } params  */
    static new(params){

        InputDropdown.injectStyles();

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
        if (params.onSelect)
            this.onSelect = async (selectedVal) => { this.#value = selectedVal; await params.onSelect(selectedVal, this) }
        if (params.onLoseFocus)
            this.onLoseFocus = async (selectedVal) => { this.#value = selectedVal; await params.onLoseFocus(selectedVal, this) }

        this.filterInput = document.querySelector(params.inputSelector);
        this.filterableList = document.querySelector(params.filterableListSelector);
        this.filterableList.classList.add(`input-filter-result-${params.componentFieldName}`)
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

    #value;
    filterList(event) {
        
        const filterText = this.filterInput.value.toLowerCase().trim();
        let matchFound = false, showAll = false;
        if(event?.key === 'Control') showAll = true;
        
        if(this.filterInput.classList.contains(invalidClass))
            this.filterInput.classList.remove(invalidClass);

        this.highlightedIndex = -1;

        for (let i = 0; i < this.listItems.length; i++) {
            const item = this.listItems[i];
            const itemText = item.textContent || item.innerText;
            item.classList.remove('highlighted');
            if(showAll) item.classList.remove('hidden');
            else {
                if (itemText.toLowerCase().includes(filterText)) {
                    item.classList.remove('hidden');
                    matchFound = true;
                }
                else item.classList.add('hidden');
            }
        }

        if ((filterText.length > 0 && matchFound) || (showAll && this.listItems.length > 0)) this.filterableList.classList.remove('hidden');
        else this.filterableList.classList.add('hidden');
        this.#value = filterText;
    }

    /** @type { String } */ getValue() { return this.#value }

    navigateList(event) {
        if (!['ArrowDown', 'ArrowUp', 'Enter'].includes(event.key)) return;
        if (this.filterableList.classList.contains('hidden')) return;

        const visible = Array.from(this.listItems).filter(li => !li.classList.contains('hidden'));
        if (!visible.length) return;

        event.preventDefault();

        if (event.key === 'Enter') {
            if (this.highlightedIndex > -1) visible[this.highlightedIndex].click();
            return;
        }

        visible[this.highlightedIndex]?.classList.remove('highlighted');
        this.highlightedIndex = event.key === 'ArrowDown'
            ? (this.highlightedIndex + 1) % visible.length : (this.highlightedIndex - 1 + visible.length) % visible.length;

        const item = visible[this.highlightedIndex];
        item.classList.add('highlighted');
        item.scrollIntoView({ block: 'nearest' });
    }

    initInputHandling() {
        const self = this;
        this.filterInput.addEventListener('input', (event) => self.filterList(event));
        this.filterInput.addEventListener('keyup', (event) => {
            if (['ArrowDown', 'ArrowUp', 'Enter'].includes(event.key)) return;
            self.filterList(event);
        });
        this.filterInput.addEventListener('keydown', (event) => self.navigateList(event));
        this.filterInput.addEventListener('blur', (event) => {
            setTimeout(() => self.filterableList.classList.add('hidden'), 150);
            if(!self.dataSource.includes(event.target.value))
                return this.filterInput.classList.add(invalidClass);
            self.onLoseFocus(event.target.value);
        });        

        this.filterableList.addEventListener('click', (event) => {
            if (String(event.target.tagName).toLowerCase() === 'li' && !event.target.classList.contains('hidden')) {
                self.filterInput.value = event.target.textContent;
                self.filterableList.classList.add('hidden');
                self.filterInput.focus();
            }
        });
    }

    onSelect = async (value, self) => {};
    onLoseFocus = async (value, self) => {};

}