export const BiUiUtil = {

    chartsTypes: [
        {id:'bar',label:'Bar',icon:'▬',cjsType:'bar'},
        {id:'line',label:'Line',icon:'╱',cjsType:'line'},
        {id:'pie',label:'Pie',icon:'◕',cjsType:'pie'},
        {id:'doughnut',label:'Donut',icon:'◎',cjsType:'doughnut'},
        {id:'scatter',label:'Scatter',icon:'⋮',cjsType:'scatter'},
        {id:'horizontalBar',label:'H-Bar',icon:'≡',cjsType:'bar'}
    ],

    chartColors: [
        '#4f6ef7',
        '#00a86b',
        '#f59e0b',
        '#ef4444',
        '#7c3aed',
        '#06b6d4',
        '#f97316',
        '#64748b'
    ],

	cellClass: (v) => {
		if (v === null || v === undefined) return "td-null";
		if (typeof v === "number") return "td-num";
		if (typeof v === "boolean") return "td-bool";
		if (typeof v === "string" && /^\d{4}-\d{2}-\d{2}/.test(v)) return "td-date";
		return "";
	},

    formatCell(v){
        if(v===null||v===undefined)return'<em>null</em>';return String(v);
    },

    showToast: (toast, msg) => {
        toast.textContent = msg;
        toast.classList.add("show");
        setTimeout(() => toast.classList.remove("show"), 3000);
    }

}