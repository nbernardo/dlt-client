export class CodeEditorUtil {

    static activeLanguage = 'python';

    static pythonSuggestions = [];

    static getPythonSuggestions() {
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
            },
            {
                label: 'SECRETS',
                kind: monaco.languages.CompletionItemKind.Keyword,
                insertText: 'SECRETS',
                documentation: 'Provide access to the diferent Secrets.',
                insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
            }
        ]
    }

    static addSecretSugestion(lang, suggestions) {

        window.monaco.languages.registerCompletionItemProvider(lang, {
            triggerCharacters: ['.'],
            provideCompletionItems(model, position) {

                const text = model.getValueInRange({
                    startLineNumber: position.lineNumber, startColumn: 1,
                    endLineNumber: position.lineNumber, endColumn: position.column
                });
                
                const isMatch = text.match(/SECRETS\.([a-zA-Z0-9_]*)$/);
                if (!isMatch) return { suggestions: [] };

                console.log(suggestions);
                return { suggestions };
            }
        });
    }

}
