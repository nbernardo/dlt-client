import { BaseController } from "../../@still/component/super/service/BaseController.js";
import { UserService } from "../services/UserService.js";
import { WorkspaceService } from "../services/WorkspaceService.js";
import { WorkSpaceController } from "./WorkSpaceController.js";

export class NoteBookController extends BaseController {

    notebookCellsContainer;
    filesOpened = new Set();
    cellCounter = 0;
    uniqueId;
    cells = {};
    noteBook;

    //This will hold the user email as this is the 
    // same way the folder was named to be unique
    userFolder;

	createCell = (id, monaco, content, language = 'python', filename = 'Untitled') => {
		const cellElement = document.createElement('div');
		cellElement.id = `cell-${id}`;
		cellElement.className = 'cell-container';

		cellElement.innerHTML = `
				<div class="drag-handle">
					<div class="drag-handle-icon" draggable="true"></div>
					<div class="move-buttons">
						<button class="move-up-btn" title="Move cell up">&#8593;</button>
						<button class="move-down-btn" title="Move cell down">&#8595;</button>
					</div>
				</div>
                <div class="add-cell-container top">
                    <button class="add-cell-above-btn cell-button add-above-btn">+</button>
                </div>
                <div class="cell-header">
                    <select class="language-select">
                        <option value="javascript">JavaScript</option>
                        <option value="python">Python</option>
                        <option value="sql">SQL</option>
                        <option value="markdown">Markdown</option>
                    </select>
                    <div class="button-group">
                        <div class="monaco-play-wrapper">
                            <button filename="${filename}" class="play-arrow-btn" aria-label="Play button">
                                <span class="tooltip-text">Run cell</span>
                            </button>
                        </div>
                        <button class="minimize-cell-btn cell-button minimize-btn">_</button>
                        <button class="delete-cell-btn cell-button delete-btn" filename="${filename}">x</button>
                    </div>
                </div>
                <div style="padding-bottom: 7px; margin-top: -6px; font-size: 13px;">${filename}</div>
                <div class="monaco-editor-container"></div>
                <div class="editor-resize-handle"></div>
                <div class="output-wrapper">
                    <h3 class="output-title">Output</h3>
                    <div class="output-container"></div>
                </div>
                <div class="add-cell-container bottom">
                    <button class="add-cell-below-btn cell-button add-below-btn">+</button>
                </div>
        `;

		const languageSelect = cellElement.querySelector('.language-select');
		languageSelect.value = language;

		const editorContainer = cellElement.querySelector('.monaco-editor-container');
		const editor = WorkSpaceController.get().loadMonadoEditor(
			editorContainer, { fontSize: 14, lang: language }
		);
		editor.setValue(content);

		this.cells[id] = { element: cellElement, editor };

		languageSelect.addEventListener('change', (e) => {
			const newLanguage = e.target.value;
			monaco.editor.setModelLanguage(editor.getModel(), newLanguage);
		});

		const resizeHandle = cellElement.querySelector('.editor-resize-handle');
		resizeHandle.addEventListener('mousedown', (e) => {
			e.preventDefault();
			const startY = e.clientY;
			const startHeight = editorContainer.offsetHeight;

			const onMouseMove = (moveEvent) => {
				const newHeight = startHeight + (moveEvent.clientY - startY);
				if (newHeight > 50) { 
					editorContainer.style.height = `${newHeight}px`;
					editor.layout();
				}
			};

			const onMouseUp = () => {
				this.noteBook.monacoEditorWrap.removeEventListener('mousemove', onMouseMove);
				this.noteBook.monacoEditorWrap.removeEventListener('mouseup', onMouseUp);
			};

			this.noteBook.monacoEditorWrap.addEventListener('mousemove', onMouseMove);
			this.noteBook.monacoEditorWrap.addEventListener('mouseup', onMouseUp);
		});

		return cellElement;
	};

	addCell = (monaco, referenceId, position, code = '', filename) => {
		const newId = this.noteBook.uniqueId + (this.cellCounter++);
		const newCellElement = this.createCell(newId, monaco, code, 'python', filename);

		if (!referenceId) {
			this.noteBook.notebookContainer.appendChild(newCellElement);
		} else {
			const referenceElement = this.cells[referenceId].element;
			if (position === 'above') {
				this.noteBook.notebookContainer.insertBefore(newCellElement, referenceElement);
			} else
				this.noteBook.notebookContainer.insertBefore(newCellElement, referenceElement.nextSibling);
		}
	};

    openFile(code, filename){
		if(!this.filesOpened.has(filename)){
			//monaco is a global object under window
        	this.addCell(window.monaco,null,null,code, filename);
			this.filesOpened.add(filename);
		}
    }

	deleteCell = (id, fileName = null, force = false) => {
		if (Object.keys(this.cells).length > 1 || force === true) { 
			this.cells[id].editor.dispose();
			this.cells[id].element.remove();
			delete this.cells[id];
			this.filesOpened.delete(fileName);
		}
	};

	removeAllCells(){
		Object.keys(this.cells).forEach(id => this.deleteCell(id, true));
	}

	runCell = async (id, fileName = null) => {
		return;
		const cellData = this.cells[id];
		const code = cellData.editor.getValue();
		const language = cellData.element.querySelector('.language-select').value;
		const outputContainer = cellData.element.querySelector('.output-container');
		outputContainer.style.display = 'block';
		outputContainer.textContent = '';
		outputContainer.style.color = '#1f2937'; 

		if (language === 'javascript') {
			try {
				let capturedOutput = '';
				const originalConsoleLog = console.log;
				console.log = (...args) => {
					capturedOutput += args.map(arg => typeof arg === 'object' ? JSON.stringify(arg, null, 2) : String(arg)).join(' ') + '\n';
				};

				eval(code);

				console.log = originalConsoleLog; // Restore original console.log
				outputContainer.textContent = capturedOutput || 'Execution completed.';
			} catch (error) {
				outputContainer.textContent = `Error: ${error.message}`;
				outputContainer.style.color = '#dc2626'; // Red for errors
			}
		} else if (language === 'markdown') {
			try {
				const html = this.noteBook.showdownConverter.makeHtml(code);
				outputContainer.innerHTML = html;
				outputContainer.classList.add('markdown-content');
			} catch (error) {
				outputContainer.textContent = `Error converting Markdown: ${error.message}`;
				outputContainer.style.color = '#dc2626';
			}
		} else if(language === 'python') {
            await (new WorkspaceService()).updatePpline(await UserService.getNamespace(), fileName, code);
			//outputContainer.textContent = `Execution for ${language} is not supported yet.`;
			//outputContainer.style.color = '#dc2626';
		}
	};

	toggleMinimize = (id) => {
		const cellData = this.cells[id];
		const editorContainer = cellData.element.querySelector('.monaco-editor-container');
		const minimizeButton = cellData.element.querySelector('.minimize-cell-btn');

		if (editorContainer.style.display === 'none') {
			editorContainer.style.display = 'block';
			minimizeButton.textContent = '_';
		} else {
			editorContainer.style.display = 'none';
			minimizeButton.textContent = '+';
		}
	};

    pythonAutoCompletionSetup(){
        return autoCompleteProvider();
    }

	moveCell = (id, direction) => {
		const cellElement = this.cells[id].element;
		const allCells = Array.from(this.noteBook.notebookContainer.children).filter(el => el.classList.contains('cell-container'));
		const currentIndex = allCells.indexOf(cellElement);

		if (direction === 'up' && currentIndex > 0) {
			const targetElement = allCells[currentIndex - 1];
			this.noteBook.notebookContainer.insertBefore(cellElement, targetElement);
		} else if (direction === 'down' && currentIndex < allCells.length - 1) {
			const targetElement = allCells[currentIndex + 1];
			this.noteBook.notebookContainer.insertBefore(cellElement, targetElement.nextSibling);
		}
	};

	draggedCell = null;
	placeholder = null;

	dragStart(){

		this.noteBook.notebookContainer.addEventListener('dragstart', (e) => {
			
			if (!e.target.classList.contains('drag-handle-icon'))
				return e.preventDefault();
	
			this.draggedCell = e.target.closest('.cell-container');
			e.dataTransfer.effectAllowed = 'move';
			e.dataTransfer.setData('text/plain', this.draggedCell.id);
	
			[this.placeholder, this.placeholder.className] = [document.createElement('div'), 'drag-placeholder'];
			setTimeout(() => this.draggedCell.classList.add('dragging'), 0);
		});

	}

	dragOver(){

		this.noteBook.notebookContainer.addEventListener('dragover', (e) => {
			e.preventDefault(), e.stopPropagation();
	
			const targetCell = e.target.closest('.cell-container');
			if (!targetCell || targetCell === this.draggedCell)
				return;
			
			const rect = targetCell.getBoundingClientRect();
			const isAbove = e.clientY < rect.top + rect.height / 2;
			
			let insertTarget;
			if (isAbove) insertTarget = targetCell;
			else insertTarget = targetCell.nextElementSibling;
	
			if (this.placeholder.parentNode !== targetCell.parentNode || this.placeholder.nextElementSibling !== insertTarget) {
				this.noteBook.notebookContainer.insertBefore(this.placeholder, insertTarget);
			}
		});
	}
	
	drop(){
		this.noteBook.notebookContainer.addEventListener('drop', (e) => {
			e.preventDefault();
			
			if (this.placeholder && this.placeholder.parentNode) 
				this.noteBook.notebookContainer.insertBefore(this.draggedCell, this.placeholder);
	
			if (this.placeholder && this.placeholder.parentNode) 
				this.placeholder.parentNode.removeChild(this.placeholder);
			
			if (this.draggedCell) {
				this.draggedCell.classList.remove('dragging');
			}
			this.draggedCell = null, this.placeholder = null;
		});
	}

	dragEnd(){

		this.noteBook.notebookContainer.addEventListener('dragend', () => {
			// Clean up if the drag was canceled
			if (this.draggedCell) {
				this.draggedCell.classList.remove('dragging');
			}
			if (this.placeholder && this.placeholder.parentNode) 
				this.placeholder.parentNode.removeChild(this.placeholder);
			
			this.draggedCell = null;
			this.placeholder = null;
		});

	}

	dragSetup(){
		this.dragStart();
		this.dragOver();
		this.drop();
		this.dragEnd();
	}

}


function autoCompleteProvider() {
	return [
		{
			label: 'def',
			kind: monaco.languages.CompletionItemKind.Keyword,
			insertText: 'def my_function(param):\n\t"""\n\tDocs here.\n\t"""\n\tpass',
			documentation: 'Defines a function',
			insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
		},
		{
			label: 'class',
			kind: monaco.languages.CompletionItemKind.Keyword,
			insertText: 'class MyClass:\n\tdef __init__(self):\n\t\tpass',
			documentation: 'Defines a class',
			insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
		},
		{
			label: 'if',
			kind: monaco.languages.CompletionItemKind.Keyword,
			insertText: 'if condition:\n\tpass',
			documentation: 'An if statement',
			insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
		},
		{
			label: 'for',
			kind: monaco.languages.CompletionItemKind.Keyword,
			insertText: 'for item in iterable:\n\tpass',
			documentation: 'A for loop',
			insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
		},
		{
			label: 'while',
			kind: monaco.languages.CompletionItemKind.Keyword,
			insertText: 'while condition:\n\tpass',
			documentation: 'A while loop',
			insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
		},
		{
			label: 'import',
			kind: monaco.languages.CompletionItemKind.Keyword,
			insertText: 'import ',
			documentation: 'Imports a module'
		},
		{
			label: 'main',
			kind: monaco.languages.CompletionItemKind.Snippet,
			insertText: 'if __name__ == "__main__":\n\t$0',
			documentation: 'Main function entry point',
			insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
		},
		{
			label: 'print',
			kind: monaco.languages.CompletionItemKind.Function,
			insertText: 'print(${1:message})',
			documentation: 'Prints to the console',
			insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
		},
		{
			label: 'len',
			kind: monaco.languages.CompletionItemKind.Function,
			insertText: 'len(${1:object})',
			documentation: 'Returns the length (number of items) of an object.',
			insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
		},
		{
			label: 'str',
			kind: monaco.languages.CompletionItemKind.Function,
			insertText: 'str(${1:object})',
			documentation: 'Returns a string version of the object.',
			insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
		},
		{
			label: 'list',
			kind: monaco.languages.CompletionItemKind.Function,
			insertText: 'list(${1:iterable})',
			documentation: 'Returns a list from an iterable.',
			insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
		}
	];
}