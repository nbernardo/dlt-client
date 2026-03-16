import { DataCatalogUI } from "./DataCatalogUI.js";


export const DataCatalogUIUtil = {

    /**  @param { DataCatalogUI } self */
    editSemantic: function (self, idx, el) {
        if (self.editingCell) return;
        
        const cols = self.PIPELINES[self.currentPipeline].tables[self.currentTable].columns;
        const visible = self.getFilteredCols();
        const col = visible[idx];
        self.editingCell = col;
        const colIdx = cols.indexOf(col);
        const input = document.createElement('input');
    
        input.className = 'semantic-edit-input';
        input.value = col.semantic || '';
        input.placeholder = 'concept name...';
        el.replaceWith(input);
        input.focus();
    
        function save() {
          const val = input.value.trim();
          cols[colIdx].semantic = val;
          
          if(cols.find(r => r.semantic != r.original_semantic) !== undefined && !(cols[colIdx].original_semantic != val)){
            self.markUnsaved(), self.editingCell = null;
            return self.renderColumns();
          }else{
            cols[colIdx].sem_source = cols[colIdx].original_source;
            self.remMarkUnsaved();
          }
          
          if(cols[colIdx].original_semantic != val){
            cols[colIdx].validated = 0;
            cols[colIdx].sem_source = 'manual';
            if (val) self.showToast(`Semantic concept "${val}" assigned`, 'success');
            self.markUnsaved();
          }
          self.editingCell = null;
          self.renderColumns();
        }
    
        input.addEventListener('blur', save);
    
        input.addEventListener('keydown', e => {
          if (e.key === 'Enter') save();
          if (e.key === 'Escape') { editingCell = null; self.renderColumns(); }
        });
    
    },

    /**  @param { DataCatalogUI } self */
    editSemanticDescription: function (self, idx, el) {
        if (self.editingCell) return;
        
        const cols = self.PIPELINES[self.currentPipeline].tables[self.currentTable].columns;
        const visible = self.getFilteredCols();
        const col = visible[idx];
        self.editingCell = col;
        const colIdx = cols.indexOf(col);
        const input = document.createElement('textarea');
    
        input.className = 'semantic-edit-input';
        input.value = col.description || '';
        input.placeholder = 'describe name...';
        el.replaceWith(input);
        input.focus();
    
        function save() {
          const val = input.value.trim();
          cols[colIdx].description = val;
          
          if(cols.find(r => r.semantic != r.original_description) !== undefined && !(cols[colIdx].original_description != val)){
            self.markUnsaved(), self.editingCell = null;
            return self.renderColumns();
          }else{
            cols[colIdx].sem_source = cols[colIdx].original_source;
            self.remMarkUnsaved();
          }
          
          if(cols[colIdx].original_description != val){
            cols[colIdx].validated = 0;
            cols[colIdx].sem_source = 'manual';
            if (val) self.showToast(`Semantic concept "${val}" assigned`, 'success');
            self.markUnsaved();
          }
          self.editingCell = null;
          self.renderColumns();
        }
    
        input.addEventListener('blur', save);
    
        input.addEventListener('keydown', e => {
          if (e.key === 'Enter') save();
          if (e.key === 'Escape') { editingCell = null; self.renderColumns(); }
        });
    
    }
}
