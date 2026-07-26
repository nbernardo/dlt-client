export function switchActiveTab(self, tab, el) {
    self.$$('.tab').forEach(b => b.classList.remove('active'));
    el.classList.add('active');
    
    if(tab){
        self.$$('.section').forEach(s => s.style.display = 'none');
        self.$('#sec-' + tab).style.display = 'block';
    }
}