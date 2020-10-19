
import sys
import time

import threading

import numpy as np

import grpc
from concurrent import futures

import tensorflow as tf
from tensorflow.python.keras.applications.resnet_v2 import ResNet50V2

from batcher.utils import resize_and_pad, resize_pad_scale, decode_b64_img, decode_and_preprocess
from batcher.batcher import Batcher, BatcherManager

import resnet50_pb2
import resnet50_pb2_grpc

_IMG_SIZE = 224
def dec_resize_pad_scale(b64_img):
    try:
        return resize_pad_scale(decode_b64_img(b64_img), _IMG_SIZE)
    except Exception as e:
        print('dec_resize_pad_scale: ', str(e))
        return np.zeros((_IMG_SIZE, _IMG_SIZE, 3))

class ResNet50Server(Batcher):

    def __init__(self,model_path, queue_size, batch_size, timeout):
        # print('ResNet50 server initializing in ', threading.current_thread().name)
        super().__init__(queue_size, batch_size, timeout)

        # Warning: Some frameworks can't do inference in one thread while initialized in the other thread
        
        return

    def model_init(self):
        config = tf.ConfigProto()
        config.gpu_options.allow_growth = False  # dynamically grow the memory used on the GPU
        config.gpu_options.visible_device_list='0'
        config.gpu_options.per_process_gpu_memory_fraction = 0.24
        config.allow_soft_placement = False
        sess = tf.Session(config=config)
        tf.keras.backend.set_session(sess)

        self.model = ResNet50V2()

    def inference(self,batch_input):
        #TODO Do inference here

        # res_array = [input + ', %d, happy?' % idx for idx, input in enumerate(batch_input)]
        
        t0 = time.time()
        img_array = [dec_resize_pad_scale(b64_img) for b64_img in batch_input]
        # print('ResNet50 preprocessing in %s costs %s' %(threading.current_thread().name, str(time.time() - t0)))
        t0 = time.time()
        img_array = np.stack(img_array)
        # print('ResNet50 batching in %s costs %s' % (threading.current_thread().name, str(time.time() - t0)))
        t0 = time.time()
        # print('ResNet50, doing inference for input of size ', img_array.shape)
        res_array = self.model.predict(img_array, batch_size=self._batch_size)
        # print('ResNet50 inference in %s costs %s' %(threading.current_thread().name, str(time.time() - t0)))
        res_array = np.argmax(res_array, axis=1)

        res_array = list(res_array)

        return res_array

class ResNet50Servicer(resnet50_pb2_grpc.ResNet50Servicer):

      def __init__(self, nbatchers, model_path, queue_size, batch_size, batch_timeout, threads_names):

        batchers = [ResNet50Server(model_path, queue_size, batch_size, batch_timeout) for _ in range(nbatchers)]
        self._batcher_manager = BatcherManager(threads_names, batchers)
        print('BatcherManager started ...')
        return

      def WhatItIs(self, request, context):
          self._batcher_manager.clean_mailbox()
          self._batcher_manager.place_order(request.b64_imgs)
          ret = self._batcher_manager.check_mailbox(5000000)

          resp = resnet50_pb2.RespResNet50(rid = request.rid, preds = ret)
          return resp


if __name__ == "__main__":

    max_workers = 200
    name_prefix = 'czh_server'
    thread_pool = futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=name_prefix)
    threads_names = [name_prefix + '_' + str(i) for i in range(max_workers)]
    
    options = [('grpc.max_message_length', 256*1024*1024),
               ('grpc.max_receive_message_length', 256*1024*1024),
               ('grpc.max_send_message_length', 256*1024*1024)]
    server = grpc.server(thread_pool, options = options)

    
    resnet50_pb2_grpc.add_ResNet50Servicer_to_server(ResNet50Servicer(4, '', max_workers, 8, 100000, threads_names), server)

    server.add_insecure_port('[::]:' + str(24356))
    server.start()

    print('ResNet50 started serving on port %d.' % 24356)
    server.wait_for_termination()
    print('ResNet50 stopped.')