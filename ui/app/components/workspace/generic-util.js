export function handleHideShowSubmenu(menuIcon, subMenuCls) {
    try {

        const dropMenu = menuIcon.querySelector('ul');
        menuIcon.onmouseover = () => dropMenu.style.display = 'block';
    
        document.addEventListener('click', (event) =>
            !dropMenu.contains(event.target) ? dropMenu.style.display = 'none' : ''
        );
    
        document.addEventListener('mouseover', (event) =>
            !dropMenu.contains(event.target) && !menuIcon.contains(event.target) ? dropMenu.style.display = 'none' : ''
        );
        
    } catch (error) {}
    
}