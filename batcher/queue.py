
import os
import time

import threading

import multiprocessing as mp
from multiprocessing.sharedctypes import RawArray, RawValue

def cur_musec():
    return int(round(time.time()*1e6))

class MMQueue:
    
    def __init__(self, qsize, block_size):
        # self._capacity = RawValue('l', size)
        # NOTE: capacity or block_size are not changed any way.
        self._blocks_num = qsize
        self._block_size = block_size

        self._blocks = [RawArray('b', block_size) for _ in range(qsize)]
        self._block_owners = [RawValue('l', -1) for _ in range(qsize)]
        self._used_cap = [RawValue('l', 0) for _ in range(qsize)]
        self._count = RawValue('l', 0)
        self._head = RawValue('l', 0)
        self._tail = RawValue('l', 0)

        self._lock = mp.Lock()
        self._cond_item_ready = mp.Condition(self._lock)
        self._cond_slot_ready = mp.Condition(self._lock)


    def put(self, owner, data, timeout=0):
        if type(owner) is not int or (type(data) is not bytes and type(data) is not bytearray):
            raise Exception('MMQueue: owner should be int, and data should be bytes')

        start_ts = cur_musec()
        b = self._lock.acquire(timeout=timeout)
        if b == False:
            raise Exception('MMQueue: timeout before acquiring the lock for put')

        while self._count.value == self._blocks_num:
            print(os.getpid(), 'waiting to put')
            cur_ts = cur_musec()
            to = max((timeout - (cur_ts - start_ts)), 1) /1e6
            b = self._cond_slot_ready.wait(to)  # Acquired the lock after awkened or timeout
            if b == False:
                self._lock.release()
                raise Exception('MMQueue: timeout before notified to put')

        # Get a slot
        

        k = self._head.value
        print(os.getpid(), ' Put ', len(data), ' at ', self._head.value)
        self._block_owners[k].value = owner
        self._blocks[k][:len(data)] = data
        self._used_cap[k].value = len(data)

        self._count.value += 1
        self._head.value = (k + 1) % self._blocks_num

        self._cond_item_ready.notify_all()
        # self._cond_slot_ready.notify_all()
        self._lock.release()
        

    def get(self, timeout=0):
        start_ts = cur_musec()
        b = self._lock.acquire(timeout=timeout)
        if b == False:
            raise Exception('MMQueue: timeout before acquiring the lock for get')

        while self._count.value == 0:
            print('*** Empty and trying to get')
            cur_ts = cur_musec()
            to = max((timeout - (cur_ts - start_ts)), 1) /1e6
            b = self._cond_item_ready.wait(to)  # Acquired the lock after awkened or timeout
            if b == False:
                self._lock.release()
                raise Exception('MMQueue: timeout before notified to get')
        
        k = self._tail.value

        item = self._blocks[k][:self._used_cap[k].value]
        owner = self._block_owners[k].value
        self._used_cap[k].value = 0

        print(os.getpid(), ' Get ', len(item), ' at ', self._tail.value)

        self._count.value -= 1
        self._tail.value = (k + 1) % self._blocks_num

        self._cond_slot_ready.notify_all()
        # self._cond_item_ready.notify_all()
        self._lock.release()

        return owner, item

    def put2(self, owners, items, timeout=0):
        if type(owners) is not list or type(items) is not list:
            raise Exception('MMQueue: owners should be int, and items should be bytes')
        
        to_be_put = len(owners)
        start_ts = cur_musec()
        b = self._lock.acquire(timeout=timeout)
        if b == False:
            raise Exception('MMQueue: timeout before acquiring the lock for put')

        while self._count.value + to_be_put > self._blocks_num:
            print(os.getpid(), 'waiting to put')
            cur_ts = cur_musec()
            to = max((timeout - (cur_ts - start_ts)), 1) /1e6
            b = self._cond_slot_ready.wait(to)  # Acquired the lock after awkened or timeout
            if b == False:
                self._lock.release()
                raise Exception('MMQueue: timeout before notified to put')

        # Get enough slots
        
        head = self._head.value
        # blocks_num = self._blocks_num.value
        print(os.getpid(), ' Put ', len(items), ' at ', self._head.value)

        for i in range(to_be_put):
            j = (head + i) % self._blocks_num
            self._block_owners[j].value = owners[i]
            self._blocks[j][:len(items[i])] = items[i]
            self._used_cap[j].value = len(items[i])

        self._count.value += to_be_put
        self._head.value = (head + to_be_put) % self._blocks_num

        self._cond_item_ready.notify_all()
        # self._cond_slot_ready.notify_all()
        self._lock.release()

    def get2(self, timeout=0, max_taken=1):
        start_ts = cur_musec()
        b = self._lock.acquire(timeout=timeout)
        if b == False:
            raise Exception('MMQueue: timeout before acquiring the lock for get')

        while self._count.value == 0:
            print('*** Empty and trying to get')
            cur_ts = cur_musec()
            to = max((timeout - (cur_ts - start_ts)), 1) /1e6
            b = self._cond_item_ready.wait(to)  # Acquired the lock after awkened or timeout
            if b == False:
                self._lock.release()
                raise Exception('MMQueue: timeout before notified to get')

        tail = self._tail.value
        
        to_be_taken = 0
        # blocks_num = self._blocks_num.value
        count = self._count.value
        if count <= max_taken:
            # Take all
            to_be_taken = count
        else:
            # Take all blocks belonging to the same owners.
            end = (tail + max_taken - 1) % self._blocks_num
            end_owner = self._block_owners[end].value
            i = max_taken
            while i < count:
                j = (tail + i) % self._blocks_num
                if self._block_owners[j].value == end_owner:
                    i += 1
                    continue
                else:
                    break
            to_be_taken = i

        items = []
        owners = []
        for i in range(to_be_taken):
            j = (tail + i) % self._blocks_num
            item = self._blocks[j][:self._used_cap[j].value]
            self._used_cap[j].value = 0
            owner = self._block_owners[j].value

            items += [item]
            owners += [owner]

        print(os.getpid(), ' Get ', len(items), ' at ', tail)
        
        self._count.value -= to_be_taken
        self._tail.value = (tail + to_be_taken) % self._blocks_num

        self._cond_slot_ready.notify_all()
        self._lock.release()

        return owner, item

    # Warning: Not process-safe
    def capacity(self):
        return self._blocks_num

    # Warning: Not process-safe
    def count(self):
        return self._count.value



if __name__ == '__main__':
    q = MMQueue(5, 1024*1024*4)
    
    def produce(queue, id, items):
        for idx, i in enumerate(items):
            queue.put2([id + idx] * 2, [i] * 2, timeout=5e6)
            time.sleep(0.01)

    def consume(queue, n, k):
        counter = {}
        for _ in range(n*k):
            t0 = time.time()
            owner, item = queue.get2(timeout=1e6)
            print(os.getpid(), ' Getting a data costs: ', len(item), ' , ', str(time.time() - t0))
            # time.sleep(0.001)
            # owner = owner.decode()
            # item = item.deocode()
            if owner not in counter:
                counter[owner] = 0

            counter[owner] += len(item)
        
        # for i in range(k):
        #     if counter[i] != n:
        #         print('Recieved: ', counter)
        #         return

        print('All items received: ', counter)
            

    # producer1 = mp.Process(target=produce, args=([1,2,3,4,5,6,7,8,9,10],))
    producer1 = threading.Thread(target=produce, args=(q, 1, [bytearray(1024*1024*4)]*12))
    producer2 = threading.Thread(target=produce, args=(q, 200, [bytearray(1024*1024)]*12))
    # producer3 = mp.Process(target=produce, args=(q, b'p3', [b'abcdefg']*10,))
    # producer4 = mp.Process(target=produce, args=(q, b'p4', [b'1234567']*6))

    consumer = mp.Process(target=consume, args=(q,12,2))

    producer1.start()
    producer2.start()
    # producer3.start()
    # producer4.start()
    consumer.start()

    producer1.join()
    producer2.join()
    # producer3.join()
    # producer4.join()
    consumer.join()

    print('Queue: ', q.count())
