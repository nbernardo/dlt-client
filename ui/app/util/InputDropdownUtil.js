import { UUIDUtil } from "../../@still/util/UUIDUtil.js";

class InputDropdownParam {
    inputSelector;
    filterableListSelector;
    dataSource;
    onSelect = null;
}

export class InputDropdown {

    dataSource = [];

    filterInput;
    filterableList;
    listItems;

    /** @param { InputDropdownParam } params  */
    static new(params){

        const resultListId = 'dynamicFilter-'+UUIDUtil.newId();
        const filterResultLst = `<ul id="${resultListId}" class="filterable-list-dropdown hidden"></ul>`;
        document.querySelector(params.inputSelector).insertAdjacentHTML('afterend',filterResultLst);
        params.filterableListSelector = `#${resultListId}`;

        return new InputDropdown(params);
        
    }

    /** @param { InputDropdownParam } params  */
    constructor(params) {
        
        if (params.dataSource) this.dataSource = params.dataSource;
        if (params.onSelect){
            this.onSelect = (selectedVal) => params.onSelect(selectedVal);
        }

        this.filterInput = document.querySelector(params.inputSelector);
        this.filterableList = document.querySelector(params.filterableListSelector);
        this.populateList();
        this.initInputHandling();
    }

    populateList() {
        const self = this;
        this.filterableList.innerHTML = '';
        this.dataSource.forEach((fruit) => {
            const li = document.createElement('li');
            li.onclick = () => self.onSelect(li.innerText);
            li.textContent = fruit;
            li.classList.add('list-item-dropdown');
            self.filterableList.appendChild(li);
        });

        this.listItems = this.filterableList.getElementsByTagName('li');
    }

    filterList() {
        const filterText = this.filterInput.value.toLowerCase().trim();
        let matchFound = false;

        for (let i = 0; i < this.listItems.length; i++) {
            const item = this.listItems[i];
            const itemText = item.textContent || item.innerText;

            if (itemText.toLowerCase().includes(filterText)) {
                item.classList.remove('hidden');
                matchFound = true;
            } else
                item.classList.add('hidden');
        }

        if (filterText.length > 0 && matchFound)
            this.filterableList.classList.remove('hidden');
        else
            this.filterableList.classList.add('hidden');
    }

    initInputHandling() {
        const self = this;
        this.filterInput.addEventListener('input', () => self.filterList());

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

    onSelect = () => {};

}