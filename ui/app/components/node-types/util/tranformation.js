
const reserved = [' and', ' or', ' not', ' None', ' else', ' if'];

export class NonDatabaseSourceTransform {

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

            if (type === 'CALCULATE')
                finalCode += `\n${field} = ${NonDatabaseSourceTransform.parseCalculate(transform)}`;

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
        return transform.replace(/\s{0,}[A-Z]{1,}/ig, (wrd, pos) => {
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

    static sourceTransformation(transformPieces, rowsConfig = []) {
        let finalCode = '';
        for (const [_, code] of [...transformPieces]) {
    
            let { type, field, transform } = code;
    
            rowsConfig.push(code);
            field = `df['${field}']`;
    
            if (type === 'CODE')
                finalCode += `\n${DatabaseTransformation.parseCodeOnDf(transform, code.field)}`;
    
            //if (type === 'CASING')
            //    finalCode += `\n${this.parseCasing(transform, code.field)}`;
    
            //if (type === 'CALCULATE')
            //    finalCode += `\n${field} = ${this.parseCalculate(transform)}`;
    
            //if (type === 'SPLIT')
            //    finalCode += `\n${this.parseSplit(field, code.sep, transform)}`;
    
            //if (type === 'DEDUP')
            //    finalCode += `\ndf.drop_duplicates(subset=['${code.field}'], inplace=True)`;
    
            //if (type === 'CONVERT') {
            //    if (transform === 'date') finalCode += `\n${field} = pd.to_datetime(${field})`;
            //    else finalCode += `\n${field} = ${field}.astype(${transform})`;
            //}
    
        }
        return finalCode;
    }

    static parseCodeOnDf(transform, field) {

        let matchCount = 0, addSpace = '', isThereBitwhise = false, totalCondition = 0;
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
            ){
                totalCondition++;
                return `${addSpace}pl.col('${wrd.trim()}')`;
            }
            else {
                const isLiteral = (wrd.startsWith("'") && wrd.endsWith("'")) || (wrd.endsWith('"') && wrd.startsWith("'"))

                if (hasSplit && side == 'right' && !isLiteral)
                    return `df.loc[condition, '${wrd.trim()}']`;
                else if (isLiteral || (transform[pos - 1]?.startsWith("'") || transform[pos - 1]?.startsWith("\"")))
                    return wrd

                totalCondition++;
                return `pl.col('${wrd.trim()}')`;
            }
        }

        let [leftSide, rightSide] = transform.split(' then ');

        const isConditionWrapped = leftSide.trim().startsWith('(') && leftSide.trim().endsWith(')')
        leftSide = leftSide?.trim()?.replace(regex, (wrd, pos) => {
            return parsePieces(wrd, pos, transform, 'left', isConditionWrapped);
        });

        matchCount = 0, addSpace = '';
        rightSide = rightSide?.trim()?.replace(regex, (wrd, pos) => {
            return parsePieces(wrd, pos, rightSide, 'right');
        });
        
        if (totalCondition < 2 || (totalCondition == 2 && isConditionWrapped))
            return `pl.when(${leftSide.replaceAll(' and ', ' & ').replaceAll(' or ', ' | ').replaceAll(`''`, `'`)})`;
        return `pl.when((${leftSide.replaceAll(' and ', ' & ').replaceAll(' or ', ' | ').replaceAll(`''`, `'`)}))`;


        //let condition = `condition = ${leftSide.replaceAll(' and ', ' & ').replaceAll(' or ', ' | ').replaceAll(`''`, `'`)}`;
        //if (isThereBitwhise) condition = `${condition})`.replace('condition = ', 'condition = (');
        //return `${condition}\ndf.loc[condition, '${field}'] = ${rightSide}`.replaceAll('( ', '(').replaceAll(') ', ')').replaceAll('  ', ' ');
    }
}



