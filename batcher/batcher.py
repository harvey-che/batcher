import time
import random

import threading
import queue

def cur_musec():
    return int(round(time.time()*1e6))

class Batcher(threading.Thread):

    def __init__(self, queue_size, batch_size, batch_timeout):
        super().__init__()
        
        self._req_queue = queue.Queue(queue_size)
        self._batch_size = batch_size
        self._batch_timeout = batch_timeout

    def recruited(self, manager):
        self._manager = manager
        
    def collect_batch(self):
        customers = []
        b64_imgs = []
        bs = 0

        start_ts = cur_musec()
        while True:
            try:
                cur_ts = cur_musec()
                remaining_to = max((self._batch_timeout - (cur_ts - start_ts)), 1) /1e6
                req = self._req_queue.get(timeout = remaining_to)

                n = len(req['items'])
                bs += n
                customers += [req['customer']] * n
                b64_imgs += req['items']
            except queue.Empty:
                # Fine
                pass
            except Exception as e:
                print('collect_batch exception: ', str(e))
                raise e

            cur_ts = cur_musec()
            
            if bs >= self._batch_size:
                # Get enough and go !
                return customers, b64_imgs
            elif bs > 0 and cur_ts > start_ts + self._batch_timeout:
                # Not enough but time to go !
                return customers, b64_imgs
            elif bs == 0 and cur_ts > start_ts + self._batch_timeout:
                # Bad luck and time to go !
                raise queue.Empty()
            else:
                # Not enough but have time. Try again !
                continue
        
        return customers, b64_imgs

    def dispatch_batch(self, customers, batch_output):
        uniq_ids = list(set(customers))
        pack = []
        for cur_id in uniq_ids:
            pack = []
            for idx, id in enumerate(customers):
                if id == cur_id:
                    pack += [batch_output[idx]]
            self._manager.deliver(cur_id, pack)

    def place_order(self, cutomer, items):
        order = {}
        order['customer'] = cutomer
        order['ts'] = cur_musec()
        order['items'] = items

        self._req_queue.put(order)
        return


    def model_init(self):
        return

    def run(self):
        self.model_init()

        while True:
            try:
                # print('Batcher trying to get a batch ...')
                customers, batch_input = self.collect_batch()
                # print('Batcher got a batch of size ', len(batch_input))

                batch_output = self.inference(batch_input)
                # print('Batcher got inference result of size', len(batch_output))

                self.dispatch_batch(customers, batch_output)
                # print('Batcher dispatched the results.')
            except Exception as e:
                print('Batcher run() in %s: %s' % (threading.current_thread().name, str(e)))
                pass

    def inference(self, batch_input):
        return []

class BatcherManager:

    def __init__(self, customers, batchers):

        self._mailboxes = {}
        for name in customers:
            self._mailboxes[name] = {}
            self._mailboxes[name]['bell'] = threading.Event()
            self._mailboxes[name]['pack'] = None

        self._batchers = batchers
        for batcher in self._batchers:
            batcher.recruited(self)
            batcher.start()

    def clean_mailbox(self):
        name = threading.current_thread().name
        
        if name not in self._mailboxes:
            raise Exception('%s\'s mailbox is gone !' % name)
        else:
            self._mailboxes[name]['bell'].clear()
            self._mailboxes[name]['pack'] = None

        return

    # Called by customer threads
    def place_order(self, items):

        c = random.randint(0, len(self._batchers) - 1)
        self._batchers[c].place_order(threading.current_thread().name, items)
        return

    # Called by cusotmer threads
    def check_mailbox(self, timeout):
        name = threading.current_thread().name
        if name not in self._mailboxes:
            raise Exception('%s\'s mailbox is gone !' % name)
        else:
            b = self._mailboxes[name]['bell'].wait(timeout = timeout / 1e6)
            if b == True:
                return self._mailboxes[name]['pack']
            else:
                raise Exception('%s\'s pack did not come in time !' % name)
    
    # Called by server threads
    def deliver(self, customer, pack):
        self._mailboxes[customer]['pack'] = pack
        self._mailboxes[customer]['bell'].set()
        return