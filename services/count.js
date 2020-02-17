const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.count = (client, from, to, callback) => {
    // Compter le nombre d'anomalies entre deux dates
    client.count({
        index: indexName,
        body: {
            query: {
                bool: {
                    filter: {
                        range: {
                            "@timestamp": {
                                "gte": from,
                                "lte": to
                            }
                        }
                    }
                }
            }
        }
    }).then(res => {
        callback({ count: res.body.count })
    })

}

exports.countAround = (client, lat, lon, radius, callback) => {
    // Compter le nombre d'anomalies autour d'un point géographique, dans un rayon donné
    client.count({
        index: indexName,
        body: {
            query: {
                bool: {
                    filter: {
                        geo_distance: {
                            distance: radius,
                            location: `${lat},${lon}`
                        }
                    }
                }
            }
        }
    }).then(res => {
        callback({ count: res.body.count })
    })
}