import { BaseController } from "../../@still/component/super/service/BaseController.js";
import { Components } from "../../@still/setup/components.js";
import { UUIDUtil } from "../../@still/util/UUIDUtil.js";
import { AppTemplate } from "../../config/app-template.js";

export class NoteBookController extends BaseController {

    notebookCellsContainer; //= document.getElementById('notebook-cells');

    createCodeCell(initialCode = '', initialTitle = 'Untitled Cell', initialLang = 'python') {
        const cell = document.createElement('div');
        cell.className = 'code-cell';
        const containerId = 'code_'+UUIDUtil.newId();

        // HTML structure for a single code cell
        cell.innerHTML = `
            <div class="p-2 text-right bg-gray-100 flex justify-between items-center">
                <div class="button-group">
                    <span contenteditable="true" class="cell-title">${initialTitle}</span>
                </div>
                <div class="button-group">
                    <select class="language-select">
                        <option value="python">Python</option>
                        <option value="sql">SQL</option>
                        <option value="javascript">JavaScript</option>
                        <option value="markdown">Markdown</option>
                    </select>
                    <button class="minimize-button control-button">-</button>
                    <button class="maximize-button control-button hidden">+</button>
                    <button class="remove-button control-button">x</button>
                </div>
            </div>

            <div class="editor-container" id="${containerId}">
                <div class="line-numbers-column"></div>
                <div class="editor-content">
                    <pre class="editor-highlighted language-${initialLang}" style="padding-left: 57px;"><code class="code-highlight"></code></pre>
                    <textarea class="editor-textarea" style="padding-left: 57px;" spellcheck="false"></textarea>
                </div>
            </div>

            <div class="output-container">
                <div class="output-area">
                    <pre>Output will appear here.</pre>
                </div>
                <div class="markdown-output hidden"></div>
            </div>
            <div class="p-2 text-right bg-gray-100 run-button-light-editor">
                <button class="run-button bg-green-500 hover:bg-green-600 text-white font-bold py-1 px-4 rounded-none text-sm shadow-none transition-colors">
                    Run Cell
                </button>
            </div>
        `;

        // Append the new cell to the notebook container
        this.notebookCellsContainer.appendChild(cell);

        // Get references to the elements inside the new cell
        const textarea = cell.querySelector('.editor-textarea');
        const codeHighlightPre = cell.querySelector('.editor-highlighted');
        const codeHighlight = cell.querySelector('.code-highlight');
        const runButton = cell.querySelector('.run-button');
        const outputArea = cell.querySelector('.output-area');
        const markdownOutput = cell.querySelector('.markdown-output');
        const removeButton = cell.querySelector('.remove-button');
        const minimizeButton = cell.querySelector('.minimize-button');
        const maximizeButton = cell.querySelector('.maximize-button');
        const editorContainer = cell.querySelector('.editor-container');
        const languageSelect = cell.querySelector('.language-select');

        // Set initial language
        languageSelect.value = initialLang;

        // Function to handle highlighting and rendering based on language
        function updateCellContent() {
            const lang = languageSelect.value;
            if (lang === 'markdown') {
                // Switch to Markdown mode
                textarea.classList.add('markdown-mode');
                codeHighlightPre.classList.add('markdown-mode');
                runButton.textContent = 'Update Preview';
                outputArea.classList.add('hidden');
                markdownOutput.classList.remove('hidden');

                // Render markdown and update the preview area
                markdownOutput.innerHTML = marked.parse(textarea.value);
            } else {
                // Switch to Code mode
                textarea.classList.remove('markdown-mode');
                codeHighlightPre.classList.remove('markdown-mode');
                runButton.textContent = 'Run Cell';
                outputArea.classList.remove('hidden');
                markdownOutput.classList.add('hidden');

                // Update syntax highlighting for code
                codeHighlightPre.className = `editor-highlighted language-${lang}`;
                codeHighlight.className = `code-highlight language-${lang}`;
                
                // Clear any existing highlighting and set plain text
                codeHighlight.innerHTML = '';
                codeHighlight.textContent = textarea.value;
                
                // Apply syntax highlighting
                if (window.Prism) {
                    Prism.highlightElement(codeHighlight);
                }
            }
        }

        this.initializeLineNumbers(document.getElementById(containerId));

        // Set initial content and highlight
        textarea.value = initialCode;
        updateCellContent();

        // Add the event listener for real-time changes
        textarea.addEventListener('input', () => {
            updateCellContent();
            syncScroll();
        });
        textarea.addEventListener('scroll', syncScroll);
        languageSelect.addEventListener('change', updateCellContent);

        // Function to sync scroll between textarea and highlighted code
        function syncScroll() {
            if (!textarea.classList.contains('markdown-mode')) {
                codeHighlightPre.scrollTop = textarea.scrollTop;
                codeHighlightPre.scrollLeft = textarea.scrollLeft;
            }
        }

        // Add a keydown event listener to handle the Tab key
        textarea.addEventListener('keydown', (e) => {
            if (e.key === 'Tab') {
                e.preventDefault();
                const start = textarea.selectionStart;
                const end = textarea.selectionEnd;
                const value = textarea.value;

                // Insert 4 spaces at the cursor position
                textarea.value = value.substring(0, start) + '    ' + value.substring(end);

                // Move the cursor to the new position
                textarea.selectionStart = textarea.selectionEnd = start + 4;
            }
        });

        // Add the event listener for the run button
        runButton.addEventListener('click', () => {
            const lang = languageSelect.value;
            if (lang === 'markdown') {
                // For Markdown, simply re-render the content
                markdownOutput.innerHTML = marked.parse(textarea.value);
            } else {
                // The Pyodide library is not included, so this button is for show
                outputArea.querySelector('pre').textContent = 'Pyodide is not included. This button is for a notebook-style aesthetic.';
            }
        });

        // Add event listener for the remove button
        removeButton.addEventListener('click', () => {
            cell.remove();
        });

        // Add event listener for the minimize button
        minimizeButton.addEventListener('click', () => {
            editorContainer.classList.add('minimized');
            minimizeButton.classList.add('hidden');
            maximizeButton.classList.remove('hidden');
        });

        // Add event listener for the maximize button
        maximizeButton.addEventListener('click', () => {
            editorContainer.classList.remove('minimized');
            maximizeButton.classList.add('hidden');
            minimizeButton.classList.remove('hidden');
        });
    }


    initializeLineNumbers(editorContainer) {
        const textarea = editorContainer.querySelector('.editor-textarea');
        const lineNumbersDiv = editorContainer.querySelector('.line-numbers-column');
        const highlightedPre = editorContainer.querySelector('.editor-highlighted');
        
        textarea.addEventListener('input', () => this.updateLineNumbers(textarea, lineNumbersDiv));
        this.syncScroll(textarea, lineNumbersDiv, highlightedPre); this.updateLineNumbers(textarea, lineNumbersDiv);
    }

    updateLineNumbers(textarea, lineNumbersDiv) {
        const lines = textarea.value.split('\n');
        const lineCount = lines.length;
        
        let lineNumbers = '';
        for (let i = 1; i <= lineCount; i++) {
            lineNumbers += i + (i < lineCount ? '<br>' : '');
        }
        
        lineNumbersDiv.innerHTML = lineNumbers;
    }
    
    syncScroll(textarea, lineNumbers, highlightedPre) {
        textarea.addEventListener('scroll', () => {
            lineNumbers.scrollTop = textarea.scrollTop;
            if (highlightedPre) {
                highlightedPre.scrollTop = textarea.scrollTop;
                highlightedPre.scrollLeft = textarea.scrollLeft;
            }
        });
    }
    
}