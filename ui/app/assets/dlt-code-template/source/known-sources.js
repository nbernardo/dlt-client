export const sourcesTemplatesMap = {
	'dlt-code': 'Code template',
	kafka_tmpl: 'Kafka',
	kafka_tmpl_sasl: 'Kafka + SASL',
	mongo_tmpl: 'MongoDB',
    translate: function(name){
        const reverseMap = { 'kafka': 'kafka_tmpl_sasl' }
        if( name in reverseMap ) return reverseMap[name];
        return name
    }
}