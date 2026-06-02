export class StringUtil {

    static snakeToCamel = (val='') => val.split('_').map(c => c.charAt(0).toUpperCase()+`${c.slice(1)}`).join(' ');

}