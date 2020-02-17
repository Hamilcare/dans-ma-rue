const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');
const chunkSize = 10000;

async function run () {
    // Create Elasticsearch client
    const client = new Client({ node: config.get('elasticsearch.uri') });

    // TODO il y a peut être des choses à faire ici avant de commencer ... 
    let body = {
        mappings: {
            properties: {
                location: { type: "geo_point" }
            }
        }
    }
    client.indices.create({ index: config.get('elasticsearch.index_name'), body: body }, (err, resp) => {
        if (err) console.trace(err.message);
    });

    let anomalies = [];
    // Read CSV file
    // fs.createReadStream('dataset/mini-rue.csv')
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {
            // TODO ici on récupère les lignes du CSV ...
            // console.log(data);
            anomalies.push({
                "@timestamp": data.DATEDECL,
                object_id: data.OBJECTID,
                annee_declaration: parseInt(data["ANNEE DECLARATION"]),
                mois_declaration: parseInt(data["MOIS DECLARATION"]),
                date_declaration: `${data["MOIS DECLARATION"]} / ${data["ANNEE DECLARATION"]}`,
                type: data.TYPE,
                sous_type: data.SOUSTYPE,
                code_postal: data.CODE_POSTAL,
                ville: data.VILLE,
                arrondissement: data.ARRONDISSEMENT,
                prefixe: data.PREFIXE,
                intervenant: data.INTERVENANT,
                conseil_de_quartier: data["CONSEIL DE QUARTIER"],
                location: data.geo_point_2d
            })
            //    console.log(data);
        })
        .on('end', async () => {
            while (anomalies.length) {
                try {
                    let resp = await client.bulk(createBulkInsertQuery(anomalies.splice(0, chunkSize)));
                    console.log(`Inserted ${resp.body.items.length} anomalies`);
                } catch (error) {
                    console.trace(error)
                }
            }
        });

}


// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery (actors) {

    const body = actors.reduce((acc, actor) => {

        const { object_id, ...params } = actor
        acc.push({ index: { _index: indexName, _type: '_doc', _id: actor.object_id } })
        acc.push(params)
        return acc
    }, []);

    return { body };
}

run().catch(console.error);
