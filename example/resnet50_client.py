
import sys
import os
import time
import random

import numpy as np

import grpc

# import common_pb2

import resnet50_pb2
import resnet50_pb2_grpc


import cv2
import glob
import base64


# from utils import resize_and_pad, encode_b64_img

def resize_and_pad(image, new_size):
    h, w = image.shape[:2]
    ratio = float(new_size) / max(h,w)

    new_hw = tuple(int(ratio * x) for x in (h,w))
    image = cv2.resize(image, (new_hw[1], new_hw[0]))  # resize expects (w,h)

    delta_h = new_size - new_hw[0]
    delta_w = new_size - new_hw[1]
    top, bottom = delta_h//2, delta_h-(delta_h//2)
    left, right = delta_w//2, delta_w-(delta_w//2)
    image = cv2.copyMakeBorder(image, top, bottom, left, right, cv2.BORDER_CONSTANT, value=[0,0,0])

    # image = cv2.copyMakeBorder(image, 0, delta_h, 0, delta_w, cv2.BORDER_WRAP)
    return image

def encode_b64_img(img):
    b64_str = cv2.imencode('.jpg', img)[1].tostring()
    b64_str = base64.b64encode(b64_str)
    return b64_str

if __name__ == '__main__':
    # import pdb; pdb.set_trace()

    # python mermaid_client.py mermaid localhost 50051 path/to/img/dir/ 4096 128
    # model = sys.argv[1]
    svr_ip = sys.argv[1]
    svr_port = sys.argv[2]
    data_dir = sys.argv[3]
    num_img = int(sys.argv[4]) # scan 'num_img' images under 'data_dir'
    batch_size = int(sys.argv[5]) # scan with this batch size
    img_path_list = glob.glob(os.path.join(data_dir, '**/*.jpg'), recursive=True)
    random.shuffle(img_path_list) # random.shuffle does its work in place.
    img_path_list = img_path_list[:num_img]
    

    options = [('grpc.max_message_length', 384*1024*1024), ('grpc.max_receive_message_length', 384*1024*1024), ('grpc.max_send_message_length', 128*1024*1024)]
    with grpc.insecure_channel(svr_ip + ':' + svr_port, options) as channel:

        # if model == 'blobfish':
        resnet50_stub = resnet50_pb2_grpc.ResNet50Stub(channel)

        img_chunk_list = [img_path_list[batch_size*i : batch_size*(i+1)] for i in range(num_img // batch_size)]
        total_time1 = 0.0
        total_time2 = 0.0
        for i in range(num_img // batch_size):
            img_chunk = img_path_list[batch_size * i : batch_size * (i + 1)]

            b64_img_array = [encode_b64_img(cv2.imread(p, cv2.IMREAD_COLOR)) for p in img_chunk]

            # img_array = common_pb2.ImageArray(width=-1, height=-1, channel = -1, data_array = b64_img_array, hash_array = [])

            # if model == 'blobfish':
            req = resnet50_pb2.ReqResNet50(rid = 'czh_resnet50_test_%d' % i , b64_imgs = b64_img_array)
            t0 = time.time()
            resp = resnet50_stub.WhatItIs(req)
            total_time1 += (time.time() - t0)
            # print('Call ResNet50.WhatItIs for %s costs %s.' % (req.rid, str(time.time() - t0)))
            # print('*** ', resp.preds)
                

        print("Average time cost per request: %f\n" % (total_time1 / (num_img // batch_size)))
        # print("Average time cost per request: %f\n" % (total_time2 / (num_img // batch_size)))
                    

            