const express = require('express');
const fs = require('fs');
const { MilvusClient } = require('@zilliz/milvus2-sdk-node');
const { DataType } = require('@zilliz/milvus2-sdk-node/dist/milvus/types/Common');
const bodyParser = require('body-parser');
const cors = require('cors')
const milvusClient = new MilvusClient('localhost:19530');
const collectionManager = milvusClient.collectionManager;
const collectionName = 'test';

const app = express();
app.use(bodyParser.json());
app.use(cors());
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'origin, content-type, accept');
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, HEAD, OPTIONS, PUT, DELETE, PATCH");
  next();
});

async function initDb() {
  const checkVersion = await milvusClient.checkVersion();
  console.log('--- check version ---', checkVersion);

  const dim = '312';
  const records = await milvusClient.dataManager.query({
    collection_name: collectionName,
    expr: `id > "0"`,
    output_fields: ["id"],
  });
  console.log('records', records.length, 'records')
  if (!records.data.length) {
    const createRes = await collectionManager.createCollection({
      collection_name: collectionName,
      //"Date", "Title", "Img", "Content", "Url", "Source", "FullVector", "TitleVector"
      fields: [
        {
          name: 'id',
          data_type: DataType.VarChar,
          is_primary_key: true,
          type_params: {
            max_length: '100',
          },
          description: '',
        },
        {
          name: 'date',
          data_type: DataType.VarChar,
          description: '',
          type_params: {
            max_length: '300',
          },
        },
        {
          name: 'link',
          data_type: DataType.VarChar,
          description: '',
          type_params: {
            max_length: '300',
          },
        },
        {
          name: 'imglink',
          data_type: DataType.VarChar,
          description: '',
          type_params: {
            max_length: '300',
          },
        },
        {
          name: 'title',
          data_type: DataType.VarChar,
          description: '',
          type_params: {
            max_length: '300',
          },
        },
        {
          name: 'vector',
          data_type: DataType.FloatVector,
          description: '',
          type_params: {
            dim,
          },
        },
        {
          name: 'title_vector',
          data_type: DataType.FloatVector,
          description: '',
          type_params: {
            dim,
          },
        },
        {
          name: 'flat_vector',
          data_type: DataType.FloatVector,
          description: '',
          type_params: {
            dim: '2',
          },
        },
      ],
    });
    const data = fs.readFileSync('./data.json', 'utf8');
    const jsondata = JSON.parse(data);
    const vectorsData = jsondata.data.map((data, id) => ({ vector: data.vector, title: data.title, link: data.link, id, imglink: data.imglink }));
    const params = {
      collection_name: collectionName,
      fields_data: vectorsData,
    };
    await milvusClient.dataManager.insert(params);
    console.log('--- Insert Data to Collection ---');

    await milvusClient.indexManager.createIndex({
      collection_name: collectionName,
      field_name: 'vector',
      extra_params: {
        index_type: 'IVF_FLAT',
        metric_type: 'L2',
        params: JSON.stringify({ nlist: 10 }),
      },
    });
    console.log('--- Create Index in Collection ---');
  }

  // need load collection before search
  await collectionManager.loadCollectionSync({
    collection_name: collectionName,
  });
}

async function getById(record_id) {
  const vector = await milvusClient.dataManager.query({
    collection_name: collectionName,
    expr: `id == "${record_id}"`,
    output_fields: ['id', 'title', 'imglink', 'vector', 'link'],
  });
  return vector.data.length ? vector.data[0] : {};

}

async function getByLink(link) {
  const vector = await milvusClient.dataManager.query({
    collection_name: collectionName,
    expr: `link == "${link}"`,
    output_fields: ['id', 'title', 'imglink', 'vector', 'link'],
  });
  return vector.data.length ? vector.data[0] : {};

}

async function getSimilar(record_id, records_num = 10) {
  const vector = await getById(record_id);
  const result = await milvusClient.dataManager.search({
    collection_name: collectionName,
    vectors: [vector.vector],
    search_params: {
      anns_field: 'vector',
      topk: records_num,
      metric_type: 'L2',
      params: JSON.stringify({ nprobe: 1024 }),
      round_decimal: 4,
    },
    output_fields: ['id', 'title', 'link', 'imglink'],
    vector_type: DataType.FloatVector,
  });
  return result.results;
}