import requests
import glob
import json
import cv2
import boto3
import argparse
import os


def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


parser = argparse.ArgumentParser()
parser.add_argument("--textonimage", type=str2bool, default=False, help="Flag to set if text is to be drawn on the image. Use true/1/yes for true and false/no/0 for false")
parser.add_argument("--jpg", type=str2bool, default=False, help="Flag to set if images are jpg format and not png. Use true/1/yes for true and false/no/0 for false")
parser.add_argument("--thresh", "-t", type=float, default=0.5, help="threshold for densecap")
args = parser.parse_args()
print(args)
if args.jpg:
    search_file_extension = ".jpg"
    out_file_extension = ".png"
else:
    search_file_extension = ".png"
    out_file_extension = ".jpg"

i = 0
api_key = "5fd560f0-3807-4c96-9b06-aee8a0baa855"

s3 = boto3.resource('s3')
bucket = 'har-model-files'
rekog = boto3.client('rekognition', 'us-west-2')

outputs = []
for img_path in glob.glob("*" + search_file_extension):  # Input images path
    output = {}
    img = cv2.imread(img_path)
    im_height, im_width, _ = img.shape
    r = requests.post("https://api.deepai.org/api/densecap", files={'image': open(img_path, 'rb')}, headers={'api-key': api_key})
    response = r.json()
    print(response)
    for cap in response['output']['captions']:
        if cap["confidence"] > args.thresh:
            color = (210, 100, 50)
            caption = cap['caption']
            print(cap['caption'], color, caption)
            bbox = cap['bounding_box']
            text_bot_left_corner = (int(bbox[0]), int(bbox[1]))
            if args.textonimage:
                cv2.putText(img, caption, text_bot_left_corner, cv2.FONT_HERSHEY_PLAIN, 1, color, 2)
            cv2.rectangle(img, (int(bbox[0]), int(bbox[1])), (int(bbox[0] + bbox[2]), int(bbox[1] + bbox[3])), color, 1)

    output['filename'] = img_path
    output['captions'] = response['output']['captions']

    print("Uploading {} to S3...".format('./' + img_path))
    s3.meta.client.upload_file('./' + img_path, bucket, img_path)
    response_ocr = rekog.detect_text(Image={"S3Object": {"Bucket": bucket, "Name": img_path, }})
    textDetections = response_ocr['TextDetections']

    for text in textDetections:
        print('Detected text:' + text['DetectedText'])
        print('Confidence: ' + "{:.2f}".format(text['Confidence']) + "%")
        print('Id: {}'.format(text['Id']))
        if 'ParentId' in text:
            print('Parent Id: {}'.format(text['ParentId']))
        print('Type:' + text['Type'])
        top_left_corner = (int(text['Geometry']['BoundingBox']["Left"] * im_width), int(text['Geometry']['BoundingBox']["Top"] * im_height))
        bottom_right_corner = (int(text['Geometry']['BoundingBox']["Left"] * im_width)+int(text['Geometry']['BoundingBox']["Width"] * im_width),
                               int(text['Geometry']['BoundingBox']["Top"] * im_height)+int(text['Geometry']['BoundingBox']["Height"] * im_height))
        print(top_left_corner, bottom_right_corner)
        if args.textonimage:
            cv2.putText(img, text['DetectedText'], top_left_corner, cv2.FONT_HERSHEY_PLAIN, 1, (15, 115, 255), 2)
        cv2.rectangle(img, top_left_corner, bottom_right_corner, (15, 115, 255), 1)

    output['ocr'] = textDetections
    outputs.append(output)
    filename = img_path.split(os.sep)[-1].split(search_file_extension)[0]  # Output Image Path
    with open(filename+'.json', 'w') as outfile:
        json.dump(output, outfile)
    cv2.imwrite(filename+"_out"+out_file_extension, img)

with open('data.json', 'w') as outfile:
    json.dump(outputs, outfile)
