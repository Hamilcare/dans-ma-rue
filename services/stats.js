const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = (client, callback) => {
    // TODO Compter le nombre d'anomalies par arondissement
    client.search({
        index: indexName,
        body: {
            size: 0,
            aggs: {
                "anoParArrondissement": {
                    terms: {
                        field: "arrondissement.keyword",
                        size: 20
                    }
                }
            }
        }
    }).then(result => {

        callback([
            result.body.aggregations.anoParArrondissement.buckets
                .map(
                    row => ({
                        arrondissement: row.key,
                        count: row.doc_count
                    })
                )
        ]);
    })

}

exports.statsByType = (client, callback) => {
    client.search({
        index: indexName,
        size: 0,
        pretty: true,
        body: {
            aggs: {
                type: {
                    terms: {
                        field: "type.keyword",
                        size: 5
                    },

                    aggs: {
                        sous_type: {
                            terms: {
                                field: "sous_type.keyword",
                                size: 5
                            }
                        }
                    }
                }
            }
        }
    }).then(result => {
        let formated = result.body.aggregations.type.buckets.map(
            typeBucket => ({
                type: typeBucket.key,
                count: typeBucket.doc_count,
                sous_types : typeBucket.sous_type.buckets.map(sousTypeBucket => ({
                    sous_type: sousTypeBucket.key,
                    count: sousTypeBucket.doc_count
                }))

            })
        )
        callback([formated]);
    })

}

exports.statsByMonth = (client, callback) => {
    // TODO Trouver le top 10 des mois avec le plus d'anomalies
    callback([]);
}

exports.statsPropreteByArrondissement = (client, callback) => {
    // TODO Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
    callback([]);
}
