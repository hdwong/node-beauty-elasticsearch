"use strict";
let core, config, logger, client, m = require('elasticsearch');

let serviceName = 'elasticsearch';
let elasticsearch = {
  assert: (error) => {
    if (error) {
      logger.error(error);
      throw '[' + serviceName + '] ' + error;
    }
  },
  init: (name, c) => {
    serviceName = name;
    core = c;
    logger = core.getLogger(serviceName);
    config = core.getConfig(serviceName);
    client = new m.Client({
      host: (config.host || '127.0.0.1') + ':' + (config.port || 9200),
    });
  },
  get_ping: (req, res, next) => {
    client.ping({
      requestTimeout: 3000,
      hello: 'elasticsearch'
    }, (error, result) => {
      next(!error && result);
    });
  },
  get_document: (req, res, next) => {
    if (req.query.query === undefined) {
      throw 'Params is wrong';
    }
    let index = req.query.index || config.name;
    let type = req.query.type || 'default';
    try {
      let queryBody = {
        index: index,
        type: type,
        body: JSON.parse(req.query.query),
        fields: []
      };
      if (req.query.from !== undefined) {
        queryBody.from = parseInt(req.query.from, 10);
      }
      if (req.query.size !== undefined) {
        queryBody.size = parseInt(req.query.size, 10);
      }
      client.search(queryBody, (error, result) => {
        elasticsearch.assert(error);
        next(result);
      });
    } catch (error) {
      elasticsearch.assert(error);
    }
  },
  post_document: (req, res, next) => {
    if (!req.body || req.body.data === undefined) {
      throw 'Params is wrong';
    }
    let index = req.body && req.body.index ? req.body.index : config.name;
    let type = req.body && req.body.type ? req.body.type : 'default';
    try {
      let docs = JSON.parse(req.body.data);
      let bulk = [];
      if (docs.length) {
        core.forEach(docs, (row) => {
          if (!row.id) {
            return;
          }
          let id = row.id;
          delete row.id;
          bulk.push({
            index: {
              _index: index,
              _type: type,
              _id: id
            }
          }, row);
        }, () => {
          client.bulk({
            body: bulk
          }, (error, result) => {
            elasticsearch.assert(error);
            next(true);
          });
        });
      } else {
        next(true);
      }
    } catch (error) {
      elasticsearch.assert(error);
    }
  },
  delete_document: (req, res, next) => {
    if (!req.body || req.body.data === undefined) {
      throw 'Params is wrong';
    }
    let index = req.body && req.body.index ? req.body.index : config.name;
    let type = req.body && req.body.type ? req.body.type : 'default';
    try {
      let docs = JSON.parse(req.body.data);
      let bulk = [];
      if (docs.length) {
        core.forEach(docs, (id) => {
          bulk.push({
            delete: {
              _index: index,
              _type: type,
              _id: id
            }
          });
        }, () => {
          client.bulk({
            body: bulk
          }, (error, result) => {
            elasticsearch.assert(error);
            next(true);
          });
        });
      } else {
        next(true);
      }
    } catch (error) {
      elasticsearch.assert(error);
    }
  }
};

module.exports = elasticsearch;

