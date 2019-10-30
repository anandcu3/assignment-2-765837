import argparse
import paramiko
import csv
import json

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--client_id', type=str, help='Set the client ID.')
    parser.add_argument('--ingest_mode', type=str, default='batch', help='Set the ingestion mode as "batch" or "nearrealtime".')
    parser.add_argument('--server_address', type=str, default='34.94.61.75')
    parser.add_argument('--file_to_upload_path', type=str, help='Set the path of the dataset to ingest including ".csv".')
    return parser.parse_args()
args = parse_args()

paramiko.util.log_to_file("paramiko.log")
host,port = args.server_address, 22
transport = paramiko.Transport((host,port))
username,password = "ftpserver","ftpserver"
transport.connect(None,username,password)
sftp = paramiko.SFTPClient.from_transport(transport)

if args.ingest_mode == "batch":
    if args.client_id == "1":
        filepath = "client-input-directory/client"+ args.client_id +"_" + args.file_to_upload_path.split("/")[-1]
        localpath = args.file_to_upload_path
        print(localpath)
        sftp.put(localpath,filepath)
    elif args.client_id == "2":
        filename = args.file_to_upload_path.split("/")[-1].split(".")[0] + ".json"
        filepath = "client-input-directory/client"+ args.client_id +"_" + filename
        print(filename)
        localpath = "../data/" + filename
        print(localpath)
        arr = []
        with open(args.file_to_upload_path,  encoding='utf-8') as csvfile:
            csvReader = csv.DictReader(csvfile)
            for rows in csvReader:
                arr.append(rows)
            with open(localpath, "w") as jsonfile:
                jsonfile.write(json.dumps(arr))
        sftp.put(localpath, filepath)
