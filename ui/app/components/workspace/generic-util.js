export function handleHideShowSubmenu(menuIconCls, subMenuCls) {
    const scheduleIcon = document.querySelector(menuIconCls);
    const dropMenu = scheduleIcon.querySelector(subMenuCls);
    scheduleIcon.onmouseover = () => dropMenu.style.display = 'block';

    document.addEventListener('click', (event) =>
        !dropMenu.contains(event.target) ? dropMenu.style.display = 'none' : ''
    );

    document.addEventListener('mouseover', (event) =>
        !dropMenu.contains(event.target) && !scheduleIcon.contains(event.target) ? dropMenu.style.display = 'none' : ''
    );
    
}