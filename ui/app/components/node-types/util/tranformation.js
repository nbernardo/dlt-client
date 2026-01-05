
const reserved = [' and', ' or', ' not', ' None', ' else', ' if'];

export class NonDatabaseSourceTransform {
    static transformTypeMap = {};
    static sourceTransformation(transformPieces, rowsConfig = []) {
        let finalCode = '';
        for (const [_, code] of [...transformPieces]) {

            let { type, field, transform } = code;

            rowsConfig.push(code);
            field = `df['${field}']`;

            if (type === 'CODE')
                finalCode += `\n${NonDatabaseSourceTransform.parseCodeOnDf(transform, code.field)}`;

            if (type === 'CASING')
                finalCode += `\n${NonDatabaseSourceTransform.parseCasing(transform, code.field)}`;

            if (type === 'CALCULATE'){
                finalCode += `\n${field} = ${NonDatabaseSourceTransform.parseCalculate(transform)})`;
                NonDatabaseSourceTransform.transformTypeMap[`${code.table}-${finalCode}`] = 'CALCULATE';
            }

            if (type === 'SPLIT')
                finalCode += `\n${NonDatabaseSourceTransform.parseSplit(field, code.sep, transform)}`;

            if (type === 'DEDUP')
                finalCode += `\ndf.drop_duplicates(subset=['${code.field}'], inplace=True)`;

            if (type === 'CONVERT') {
                if (transform === 'date') finalCode += `\n${field} = pd.to_datetime(${field})`;
                else finalCode += `\n${field} = ${field}.astype(${transform})`;
            }

        }
        return finalCode;
    }

    static parseCodeOnDf(transform, field) {

        let matchCount = 0, addSpace = '', isThereBitwhise = false;
        const isMultiConditionCode = [' and ', ' or '].includes(transform);
        const regex = /.split\([\s\S]{1,}\)\[[0-9]{1,}\]|\.[A-Z]{1,}\(|s{0,}\'[\sA-Z]{1,}\'|\s{0,}[A-Z]{1,}/ig;
        const hasSplit = transform.indexOf('.split(') > 0;

        const parsePieces = (wrd, pos, transform, side = null) => {
            matchCount++;

            // In case split is being used
            if (wrd.indexOf('.split(') == 0 && wrd.endsWith(']'))
                return wrd.replace('.', '.str.').replace(')[', ').str[');

            if (wrd.startsWith('.') && wrd.endsWith('('))
                return wrd.replace('.', '.str.');

            if (wrd.trim() == "' '") return wrd

            const isWordBitwise = ['and', 'or'].includes(wrd.trim());
            if (isWordBitwise) {
                isThereBitwhise = true;
                return `) ${wrd} (`;
            }
            if (matchCount > 1) addSpace = ' ';

            if (
                pos === 0 && !wrd.startsWith("'") && !wrd.startsWith("\"") && !transform[pos - 1]?.startsWith("'")
                //|| pos > 0 && !transform[pos - 1]?.startsWith("'") && !transform[pos - 1]?.startsWith("\"")
            )
                return isMultiConditionCode ? `(${addSpace}df['${wrd.trim()}']` : `${addSpace}df['${wrd.trim()}']`;
            else {
                const isLiteral = (wrd.startsWith("'") && wrd.endsWith("'")) || (wrd.endsWith('"') && wrd.startsWith("'"))

                if (hasSplit && side == 'right' && !isLiteral)
                    return `df.loc[condition, '${wrd.trim()}']`;
                else if (isLiteral || (transform[pos - 1]?.startsWith("'") || transform[pos - 1]?.startsWith("\"")))
                    return wrd
                return `df['${wrd.trim()}']`;
            }
        }

        let [leftSide, rightSide] = transform.split(' then ');
        leftSide = leftSide?.trim()?.replace(regex, (wrd, pos) => {
            return parsePieces(wrd, pos, transform);
        });

        matchCount = 0, addSpace = '';
        rightSide = rightSide?.trim()?.replace(regex, (wrd, pos) => {
            return parsePieces(wrd, pos, rightSide, 'right');
        });

        let condition = `condition = ${leftSide.replaceAll(' and ', ' & ').replaceAll(' or ', ' | ').replaceAll(`''`, `'`)}`;
        if (isThereBitwhise) condition = `${condition})`.replace('condition = ', 'condition = (');
        return `${condition}\ndf.loc[condition, '${field}'] = ${rightSide}`.replaceAll('( ', '(').replaceAll(') ', ')').replaceAll('  ', ' ');
    }

    static parseCode(transform, field) {

        transform = transform.replace(/\s{0,}[A-Z]{1,}/ig, (wrd, pos) => {

            if (reserved.includes(wrd)) return `${wrd} `;

            if (pos === 0 && !wrd.startsWith("'") && !wrd.startsWith("\"") && !transform[pos - 1].startsWith("'"))
                return `df['${wrd.trim()}']`;
            else if (pos > 0 && !transform[pos - 1].startsWith("'") && !transform[pos - 1].startsWith("\""))
                return `df['${wrd.trim()}']`;
            else return wrd;

        });

        return `${field} = ${transform}`
    }

    static parseCalculate(transform) {
        return transform.replace(/\s{0,}[A-Z0-9]{1,}/ig, (wrd, pos) => {
            if (pos === 0 && !wrd.startsWith("'") && !wrd.startsWith("\""))
                return ` df['${wrd.trim()}'] `;
            else if (pos > 0 && !transform[pos - 1].startsWith("'") && !transform[pos - 1].startsWith("\""))
                return ` df['${wrd.trim()}'] `;
            else return wrd;
        });
    }

    static parseSplit(field, sep, vars) {
        vars = vars.replace(/\[|\]/g, '').split(',');
        let content = `${vars} = ${field}.str.split('${sep}', n=${vars.length})\n`;

        for (const field of vars)
            content += `df['${field.trim()}'] = ${field}\n`;

        return `${content}`;
    }

    static parseCasing(transform, field) {
		return 'UPPER' === transform
			? `df['${field}'] =  df['${field}'].str.upper()`
			: 'LOWER' === transform ? `df['${field}'] = df['${field}'].str.lower()`
				: `# df['${field}'] Unchenged case`;
	}

}



export class DatabaseTransformation {

    /** @type { Object<Array> } */
    static transformations = {};
    static otherTransformations = {};
    static transformTypeMap = {};

    static sourceTransformation(transformPieces, rowsConfig = []) {
        let finalCode = '', comma = null;
        DatabaseTransformation.transformations = {};
        DatabaseTransformation.otherTransformations = {};
        for (const [_, code] of [...transformPieces]) {
            let finalCode = '';
            let { type, field, transform, table, isNewField } = code;
    
            rowsConfig.push(code);
            comma = '';//finalCode.length > 0 ? ',\n' : '';

            if(type === 'CODE' && code === '') return '';
            if (type === 'CODE'){
                const transformation = DatabaseTransformation.parseCodeOnDf(transform, code.field, isNewField)
                if(transformation) finalCode = `${comma}${transformation}`;
                DatabaseTransformation.transformTypeMap[`${code.table}-${finalCode}`] = { type: 'CODE', isNewField };
            }

            if (type === 'FILTER'){
                const transformation = DatabaseTransformation.parseFilterOnDf(transform, code.field)
                if(transformation) finalCode = `${comma}${transformation}`;
                DatabaseTransformation.updateOtherTransform(table, finalCode, code, 'FILTER', isNewField);
            }
    
            if (type === 'CASING'){
                finalCode = `${comma}${DatabaseTransformation.parseCasing(transform, code.field)}`;
                DatabaseTransformation.transformTypeMap[`${code.table}-${finalCode}`] = { type: 'CASING', isNewField };
            }
    
            if (type === 'CALCULATE'){
                finalCode = `${comma}(${DatabaseTransformation.parseCalculate(transform)}).alias('${code.field}')`
                        .replaceAll('( ','(',)
                        .replaceAll(') ',')',)
                        .replace(/\s{2,}/g,' ');
                DatabaseTransformation.transformTypeMap[`${code.table}-${finalCode}`] = { type: 'CALCULATE', isNewField };
            }
    
            if (type === 'SPLIT'){
                finalCode = `${comma}${DatabaseTransformation.parseSplit(field, code.sep, transform)}`;
                DatabaseTransformation.transformTypeMap[`${code.table}-${finalCode}`] = { type: 'SPLIT', isNewField };
            }

            if (type === 'DEDUP'){
                if(finalCode.indexOf('df.unique(subset=[') >= 0){
                    finalCode +=  finalCode.replace('df.unique(subset=[',`df.unique(subset=['${code.field}',`);
                }else{
                    finalCode += `df.unique(subset=['${code.field}'])`;
                }
                DatabaseTransformation.updateOtherTransform(table, finalCode, code, 'DEDUP', isNewField);
            }
    
            if (type === 'DROP'){
                if(finalCode.indexOf('df.drop([') >= 0){
                    finalCode +=  finalCode.replace('df.drop([',`df.drop(['${code.field}',`);
                }else{
                    finalCode += `df.drop(['${code.field}'])`;
                }
                DatabaseTransformation.updateOtherTransform(table, finalCode, code, 'DROP', isNewField);
            }

            //if (type === 'CONVERT') {
            //    if (transform === 'date') finalCode += `\n${field} = pd.to_datetime(${field})`;
            //    else finalCode += `\n${field} = ${field}.astype(${transform})`;
            //}

            if(!(table in DatabaseTransformation.transformations))
                DatabaseTransformation.transformations[table] = []

            const prevVal = DatabaseTransformation.transformations[table];
            DatabaseTransformation.transformations[table] = [...prevVal, finalCode];
    
        }
        return finalCode;
    }

    static updateOtherTransform(table, finalCode, code, type, isNewField){
        if(!(table in DatabaseTransformation.otherTransformations))
            DatabaseTransformation.otherTransformations[table] = []

        const prevVal = DatabaseTransformation.otherTransformations[table];
        DatabaseTransformation.otherTransformations[table] = [...prevVal, `lambda df: ${finalCode}`];
        DatabaseTransformation.transformTypeMap[`${code.table}-${finalCode}`] = { type, isNewField };
    }

    static parseCasing(transform, field) {
        
		return 'UPPER' === transform
			? `pl.col('${field}').str.to_uppercase().alias('${field}')`
			: 'LOWER' === transform ? `pl.col('${field}').str.to_lowercase().alias('${field}')`
				: `# pl.col('${field}') Unchenged case`;
	}

    static parseCalculate(transform = '') {
        if(transform === undefined) return null;
        return transform
            .replace(/\s{0,}[A-Z]{1,}[0-9]{0,}/ig, (wrd, pos) => {
                if (pos === 0 && !wrd.startsWith("'") && !wrd.startsWith("\""))
                    return ` pl.col('${wrd.trim()}') `;
                else if (pos > 0 && !transform[pos - 1].startsWith("'") && !transform[pos - 1].startsWith("\""))
                    return ` pl.col('${wrd.trim()}') `;
                else return wrd;
            })
            .replace(/[0-9\s\*\+\-\/\%\^]{3,}/, (wrd) => {
                if(/[A-Za-z]{1,}/.test(transform)) return wrd;
                else return `pl.lit(${wrd})`;
            });
    }

    static parseSplit(field, sep, vars) {
        vars = vars.replace(/\[|\]/g, '').split(',');
        let content = ''//`${vars} = ${field}.str.split('${sep}', n=${vars.length})\n`;

        let x = 0, total = vars.length, c = '';
        for (const newField of vars){
            c = content.length > 0 ? ',' : ''
            const fname = newField.trim();
            //if((x + 1) === total)
            content += `${c}pl.col('${field}').fill_null('UNKNOWN').str.split('${sep}').list.get(${x}).alias('${fname}')`
            //else
            //    content += `${c}pl.col('${field}').cast(pl.Utf8).fill_null(" - ").str.split('${sep}').arr.slice(${x},${x+1}).alias('${fname}')`
            x++;
        }

        return `${content}`;
    }

    static parseCodeOnDf(transform, field, isNewField) {

        let matchCount = 0, addSpace = '', isThereBitwhise = false, totalCondition = 0;
        let prevStrManipulation = null;
        const regex = /.split\([\s\S]{1,}\)\[[0-9]{1,}\]|\.[A-Z0-9\_]{1,}\(|s{0,}\'[^']{1,}\'|\s{0,}[A-Z0-9\_]{1,}/ig;
        let wasThenAdded = false;

        const parsePieces = (wrd, pos, transform, side = null) => {
            matchCount++;

            let splitSnippet = wrd.indexOf('.split(') == 0 && wrd.endsWith(']');
            const wrdStartPos = pos + wrd.length;
            const isReplaceContent = 
                transform.slice(pos - 8).startsWith('replace(')
                || transform.slice(pos - 8).startsWith('ace_all(')
                || transform.replaceAll(' ','').slice(pos - 1).startsWith(',');

            if(transform.slice(wrdStartPos, wrdStartPos + 10).startsWith('.split')){
                prevStrManipulation = wrd;
                return '';
            }
            if (wrd.startsWith('.') && wrd.endsWith('(')){
                if(wrd.includes('is_null') || wrd.includes('is_not_null'))
                    return wrd;
                return wrd.replace('.', '.str.');
            }

            const isWordBitwise = ['and', 'or'].includes(wrd.trim());
            if (isWordBitwise) {
                isThereBitwhise = true;
                return `) ${wrd} (`;
            }
            if (matchCount > 1) addSpace = ' ';

            if (
                pos === 0 && !wrd.startsWith("'") && !wrd.startsWith("\"") && !transform[pos - 1]?.startsWith("'")
                //|| pos > 0 && !transform[pos - 1]?.startsWith("'") && !transform[pos - 1]?.startsWith("\"")
            ){
                totalCondition++;
                if(isReplaceContent)
                    return wrd.trim();
                return `${addSpace}pl.col('${wrd.trim()}')`;
            }
            else {
                const isLiteral = (wrd.startsWith("'") && wrd.endsWith("'")) || (wrd.endsWith('"') && wrd.startsWith("'"))

                if (side == 'right' && !isLiteral && splitSnippet){
                    wasThenAdded = true;
                    return wrd
                            .replace('.split(',`pl.col('${prevStrManipulation}').str.split(`)
                            .replace('[','.arr.get(')
                            .replace(']',')');
                }
                if (side == 'right' && isLiteral){
                    if(isReplaceContent)
                        return wrd.trim();
                    else{
                        return `pl.lit(${wrd == "" ? '' : wrd.trim()})`;
                    }
                }
                else if (isLiteral || (transform[pos - 1]?.startsWith("'") || transform[pos - 1]?.startsWith("\""))){
                    return wrd
                }

                totalCondition++;
                if(!isNaN(wrd.trim())) return wrd;
                return `pl.col('${wrd.trim()}')`;
            }
        }

        if(transform === undefined) return null;

        let [leftSide, rightSide] = transform.split(' then ');

        const isConditionWrapped = leftSide.trim().startsWith('(') && leftSide.trim().endsWith(')')
        leftSide = leftSide?.trim()?.replace(regex, (wrd, pos) => {
            return parsePieces(wrd, pos, leftSide, 'left', isConditionWrapped);
        });

        matchCount = 0, addSpace = '';
        rightSide = rightSide?.trim()?.replace(regex, (wrd, pos) => {
            return parsePieces(wrd, pos, rightSide, 'right');
        });
        
        if (totalCondition < 2 || (totalCondition == 2 && isConditionWrapped))
            leftSide = `pl.when(${leftSide.replaceAll(' and ', ' & ').replaceAll(' or ', ' | ').replaceAll(`''`, `'`)})`;
        else
            leftSide = `pl.when((${leftSide.replaceAll(' and ', ' & ').replaceAll(' or ', ' | ').replaceAll(`''`, `'`)}))`;

        if(rightSide === undefined) return null;

        //Addressing no space concatenation
        rightSide = rightSide.replace(/\s{0,}\+\'\'/g,`+pl.lit('')`);
        //Address change case from the code
        rightSide = rightSide.replaceAll('.lower()','.to_lowercase()');
        rightSide = rightSide.replaceAll('.upper()','.to_uppercase()');
        leftSide = leftSide.replaceAll('.lower()','.to_lowercase()');
        leftSide = leftSide.replaceAll('.upper()','.to_uppercase()');
        //Additional string manipulation
        rightSide = rightSide.replaceAll('.strip(','.strip_chars(');

        if(!rightSide.includes('.then(')) rightSide = `.then(${rightSide}`;
        if(isNewField)
            return `${leftSide}${rightSide}).otherwise(None).alias('${field}')`;
        return `${leftSide}${rightSide}).otherwise(pl.col('${field}')).alias('${field}')`;

    }

    /** @param {String} transform */
    static parseFilterOnDf(transform) {

        let matchCount = 0, addSpace = '', isThereBitwhise = false, totalCondition = 0;
        let prevStrManipulation = null;
        const regex = /.split\([\s\S]{1,}\)\[[0-9]{1,}\]|\.[A-Z0-9\_]{1,}\(|s{0,}\'[^']{1,}\'|\s{0,}[A-Z0-9\_]{1,}[0-1]{0,}/ig;
        let wasThenAdded = false;

        const parsePieces = (wrd, pos, transform, side = null) => {
            matchCount++;

            let splitSnippet = wrd.indexOf('.split(') == 0 && wrd.endsWith(']');
            const wrdStartPos = pos + wrd.length;
            const isReplaceContent = 
                transform.slice(pos - 8).startsWith('replace(')
                || transform.slice(pos - 8).startsWith('ace_all(')
                || transform.replaceAll(' ','').slice(pos - 1).startsWith(',');

            if(transform.slice(wrdStartPos, wrdStartPos + 10).startsWith('.split')){
                prevStrManipulation = wrd;
                return '';
            }
            if (wrd.startsWith('.') && wrd.endsWith('(')){
                if(wrd.includes('is_null') || wrd.includes('is_not_null'))
                    return wrd;
                return wrd.replace('.', '.str.');
            }

            const isWordBitwise = ['and', 'or'].includes(wrd.trim());
            if (isWordBitwise) {
                isThereBitwhise = true;
                return `) ${wrd} (`;
            }
            if (matchCount > 1) addSpace = ' ';

            if (
                pos === 0 && !wrd.startsWith("'") && !wrd.startsWith("\"") && !transform[pos - 1]?.startsWith("'")
            ){
                totalCondition++;
                if(isReplaceContent || !isNaN(wrd.trim()))
                    return wrd.trim();
                return `${addSpace}pl.col('${wrd.trim()}')`;
            }
            else {
                const isLiteral = (wrd.startsWith("'") && wrd.endsWith("'")) || (wrd.endsWith('"') && wrd.startsWith("'"))

                if (side == 'right' && !isLiteral && splitSnippet){
                    wasThenAdded = true;
                    return wrd
                            .replace('.split(',`pl.col('${prevStrManipulation}').str.split(`)
                            .replace('[','.arr.get(')
                            .replace(']',')');
                }
                if (side == 'right' && isLiteral){
                    if(isReplaceContent)
                        return wrd.trim();
                    else{
                        return `pl.lit(${wrd == "" ? '' : wrd.trim()})`;
                    }
                }
                else if (isLiteral || (transform[pos - 1]?.startsWith("'") || transform[pos - 1]?.startsWith("\""))){
                    return wrd
                }

                totalCondition++;
                if(!isNaN(wrd.trim())) return wrd;
                return `pl.col('${wrd.trim()}')`;
            }
        }

        if(transform === undefined) return null;

        matchCount = 0, addSpace = '';
        transform = transform?.trim()?.replace(regex, (wrd, pos) => {
            return parsePieces(wrd, pos, transform, 'right');
        });

        //Addressing no space concatenation
        transform = transform.replace(/\s{0,}\+\'\'/g,`+pl.lit('')`);
        //Address change case from the code
        transform = transform.replaceAll('.lower()','.to_lowercase()');
        transform = transform.replaceAll('.upper()','.to_uppercase()');
        //Additional string manipulation
        transform = transform.replaceAll('.strip(','.strip_chars(');
        transform = transform
            .replace(/\)\s{1,}and\s{1,}(pl\.|\(pl.)|\)\s{1,}and\s{1,}\(/ig,(_, $1) => {
                return ` & ${$1}`;
            })
            .replace(/\)\s{1,}or\s{1,}(pl\.|\(pl.)|\)\s{1,}or\s{1,}\(/ig,(_, $1) => {
                return ` | ${$1}`;
            })
        const openingParethesis = transform.match(/\(/g).length || 0;
        const closingParethesis = transform.match(/\)/g).length || 0;
        const totalClosingGap = openingParethesis - closingParethesis;

        return `df.filter((${transform}))${')'.repeat(totalClosingGap)}`;

    }
}

export class TransformExecution {

    static transformPreviewShow = (cmpId, result) =>
        document.querySelector(`.${cmpId}-previewPlaceholder`).innerHTML = result;
    
    static validationErrDisplay = (rowId, error) => {
        const css = 'color: red; display: block; text-align: right; padding-right: 21px; color: red';
		document.getElementById(`row_error_${rowId}`).innerHTML = `<span style='${css}'>${error} -> </span>`;
        
		document.getElementById(`${rowId}-codeTransform`).classList.add('transformation-code-error');
        document.getElementById(`${rowId}-filterTransform`).classList.add('transformation-code-error');
        document.getElementById(`${rowId}-calcTransform`).classList.add('transformation-code-error');
    }
    
    static validationErrDisplayReset = (cmpId) => {
		const cntr = document.querySelector(`.${cmpId}`);
        if(cntr){
            cntr.querySelectorAll('.transformation-code-error').forEach(codeEditor => {
                const id = codeEditor.id;
                codeEditor.classList.remove('transformation-code-error');
            });

            cntr.querySelectorAll('.error-message-placeholder').forEach(msgWrapper => {
                msgWrapper.innerHTML = '';
            });
        }
    }
}

