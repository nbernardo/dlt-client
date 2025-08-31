export class MoreOptionsMenu {

    popup;
    container;
    /** @type { DataSourceFields } */
    fieldsMenu;

    /** @returns { MoreOptionsMenu } */
    handleShowPopup(e, containerId) {
        
        this.popup = document.querySelector('.popup-right-sede-'+containerId);
        this.container = e.target.parentElement;
        if (this.popup.contains(e.target)) return;

        const containerRect = this.container.getBoundingClientRect();
        const x = e.clientX - containerRect.left;
        const y = e.clientY - containerRect.top;
        
        this.showPopup(x, y, this);
        e.stopPropagation();

        return this;
    };


    showPopup(x, y, obj) {
            
        const popupWidth = 200, popupHeight = 520, margin = 10;
        const containerRect = obj.container.getBoundingClientRect();
        const containerWidth = containerRect.offsetWidth;
        const containerHeight = containerRect.offsetHeight;

        let left = x + margin;
        let top = y - (popupHeight / 2);

        if (left + popupWidth > containerWidth) left = x - popupWidth - margin;
        if (left < 0) left = margin;
        
        if (top < 0) top = margin;
        if (top + popupHeight > containerHeight) top = containerHeight - popupHeight - margin;
        
        obj.popup.style.left = left + 'px';
        obj.popup.style.top = (top+105) + 'px';
        obj.popup.style.display = 'block';

    }
}


export class DataSourceFields{
    // Sample Data for field format
    fields = [{ id: 1, name: 'id', type: 'integer' }];

    static dataTypes = {
        string: { icon: 'Aa', label: 'String' },
        integer: { icon: '123', label: 'Integer' },
        boolean: { icon: '✓', label: 'Boolean' },
        datetime: { icon: '📅', label: 'DateTime' },
        text: { icon: 'Txt', label: 'Text' }
    };

    containerId;
    nextId = 6;
    static currentMenu = null;

    changeFieldType(fieldId, newType) {
        const field = this.fields.find(f => f.id === fieldId);
        if (field) {
            field.type = newType;
            this.render();
        }
        this.hideTypeMenu();
    }

    addField(fields) {
        const newFields = [
            { name: 'phone', type: 'string' },
            { name: 'age', type: 'integer' },
            { name: 'updated_at', type: 'datetime' },
            { name: 'is_verified', type: 'boolean' },
            { name: 'description', type: 'text' }
        ];
        
        const randomField = newFields[Math.floor(Math.random() * newFields.length)];
        this.fields.push({
            id: fields.length++,
            name: randomField.name,
            type: randomField.type
        });
        this.render();
        return randomField.name;
    }

    hideTypeMenu() {
        if (DataSourceFields.currentMenu) {
            DataSourceFields.currentMenu.remove();
            DataSourceFields.currentMenu = null;
        }
    }

    showTypeMenu(fieldId, iconElement, filesList, popupId, cb, obj) {
        this.hideTypeMenu();
        
        const menu = document.createElement('div');
        menu.className = 'type-menu';
        menu.style.left = iconElement.offsetLeft + 'px';
        menu.style.top = iconElement.offsetTop + iconElement.offsetHeight + 5 + 'px';
        
        Object.keys(DataSourceFields.dataTypes).forEach(type => {
            const option = document.createElement('div');
            option.className = 'type-option';
            option.innerHTML = `
                <div class="mini-icon ${type}">${DataSourceFields.dataTypes[type].icon}</div>
                ${DataSourceFields.dataTypes[type].label}
            `;
            option.onclick = () => cb(popupId, filesList, obj, fieldId, type);
            menu.appendChild(option);
        });
        
        iconElement.parentElement.appendChild(menu);
        DataSourceFields.currentMenu = menu;
    }

}