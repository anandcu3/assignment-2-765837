def ingestion(batch_path, client_id):

    import pymongo, time, os, bson
    import pandas as pd

    mongo_client = pymongo.MongoClient('mongodb://anandcu3:h3GuvlswXSUsRgCb@cluster0-shard-00-00-fbaws.gcp.mongodb.net:27017,cluster0-shard-00-01-fbaws.gcp.mongodb.net:27017,cluster0-shard-00-02-fbaws.gcp.mongodb.net:27017/admin?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority')
    database = mongo_client['google_play_store']
    if client_id == "customer1":
        table_name = "app_information"
        client = "customer1"
        table = db["app_information"]
    elif if client_id == "customer2":
        table_name = "app_reviews"
        client = "customer2"
        table = db["app_reviews"]

    batch = pd.read_csv(batch_path)
    start = time.time()
    request = table.insert(batch.to_dict(orient='records'))
    end = time.time()

    collection_stats = database.command("collstats", client_id)

    report = {
            'ingestion_size_local':os.path.getsize(batch_path),
            'ingestion_size_MongoDB':sum([len(bson.BSON.encode(table.find_one({'_id':document_id}))) for document_id in request]),
            'ingestion_time':end-start,
            'successful_rows':len(request),
            'successful_rows_rate':len(request)/batch.shape[0],
            'collection_size':collection_stats['size'],
            'collection_count':collection_stats['count'],
            'collection_avgObjSize':collection_stats['avgObjSize']
            }
    print('the ingestion report is:', file=open("clientbatchingestapp.log","a"))
    del batch, start, end, request, collection_stats, table, database, mongo_client, client_id

    return report
