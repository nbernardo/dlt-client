export class MoreOptionsMenu {

    popup;
    container;
    /** @type { DataSourceFields } */
    fieldsMenu;

    setupMoreOptions(e, containerId){
    
        this.popup = document.querySelector('.popup-right-sede-'+containerId);
        this.container = e.target.parentElement;
        this.fieldsMenu = new DataSourceFields(containerId);
        const obj = this;
    
        this.container.addEventListener('click', function(e) {
            if (obj.popup.contains(e.target)) return;
            
            const containerRect = obj.container.getBoundingClientRect();
            const x = e.clientX - containerRect.left;
            const y = e.clientY - containerRect.top;
            
            showPopup(x, y);
            e.stopPropagation();
        });
    
        e.target.addEventListener('click', function(e) {
            if (obj.popup.style.display === 'block' && !obj.popup.contains(e.target))
                hidePopup();
        }, true);
    
        function showPopup(x, y) {
            
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
    
        function hidePopup() {
            obj.popup.style.display = 'none';
        }
    
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') hidePopup();
        });
    
    }
}


export class DataSourceFields{

    fields = [
        { id: 1, name: 'id', type: 'integer' },
        { id: 2, name: 'name', type: 'string' },
        { id: 3, name: 'email', type: 'string' },
        { id: 4, name: 'active', type: 'boolean' },
        { id: 5, name: 'created_at', type: 'datetime' }
    ];

    dataTypes = {
        string: { icon: 'Aa', label: 'String' },
        integer: { icon: '123', label: 'Integer' },
        boolean: { icon: '✓', label: 'Boolean' },
        datetime: { icon: '📅', label: 'DateTime' },
        text: { icon: 'Txt', label: 'Text' }
    };

    containerId;

    constructor(containerId){

        // document.addEventListener('click', (e) => {
        //     if (!e.target.closest('.icon') && !e.target.closest('.type-menu')) {
        //         hideTypeMenu();
        //     }
        // });
        this.containerId = containerId;
        this.render(containerId);

    }

    nextId = 6;
    currentMenu = null;

    showTypeMenu(fieldId, iconElement) {
        this.hideTypeMenu();
        
        const menu = document.createElement('div');
        menu.className = 'type-menu';
        menu.style.left = iconElement.offsetLeft + 'px';
        menu.style.top = iconElement.offsetTop + iconElement.offsetHeight + 5 + 'px';
        
        Object.keys(this.dataTypes).forEach(type => {
            const option = document.createElement('div');
            option.className = 'type-option';
            option.innerHTML = `
                <div class="mini-icon ${type}">${this.dataTypes[type].icon}</div>
                ${this.dataTypes[type].label}
            `;
            option.onclick = () => this.changeFieldType(fieldId, type);
            menu.appendChild(option);
        });
        
        iconElement.parentElement.appendChild(menu);
        this.currentMenu = menu;
    }

    hideTypeMenu() {
        if (this.currentMenu) {
            this.currentMenu.remove();
            this.currentMenu = null;
        }
    }

    changeFieldType(fieldId, newType) {
        const field = this.fields.find(f => f.id === fieldId);
        if (field) {
            field.type = newType;
            render();
        }
        this.hideTypeMenu();
    }

    render(containerId) {
        const container = document.querySelector('.fields-'+containerId);
        container.innerHTML = '';
        
        this.fields.forEach(field => {
            const div = document.createElement('div');
            div.className = 'field';
            div.innerHTML = `
                <div class="icon ${field.type}" onclick="showTypeMenu(${field.id}, this)">
                    ${this.dataTypes[field.type].icon}
                </div>
                <span class="name">${field.name}</span>
            `;
            container.appendChild(div);
        });
    }

    addField() {
        const newFields = [
            { name: 'phone', type: 'string' },
            { name: 'age', type: 'integer' },
            { name: 'updated_at', type: 'datetime' },
            { name: 'is_verified', type: 'boolean' },
            { name: 'description', type: 'text' }
        ];
        
        const randomField = newFields[Math.floor(Math.random() * newFields.length)];
        this.fields.push({
            id: this.nextId++,
            name: randomField.name,
            type: randomField.type
        });
        this.render(this.containerId);
    }

}