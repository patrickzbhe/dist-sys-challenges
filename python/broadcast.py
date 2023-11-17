#!/usr/bin/env python3
from node import Net
import time
import uuid
import sys

class BroadcastNode:
    def __init__(self):
        self.net = Net(retry_delay=3)
        self.node_id = None
        self.stored_broadcasts = set()
        self.topology = None
        self.cur_time = time.time()
        self.setup_handlers()
        self.id_numeric = None
        self.send_buffer = []
        self.last_flushed = time.time()

    def set_node_id(self, id):
        self.node_id = id
        self.net.node_id = id
        self.id_numeric = int(''.join([x for x in id if x.isnumeric()]))

    def setup_handlers(self):
        def broadcast_init(msg):
            body = msg['body']
            self.set_node_id(body['node_id'])
            self.net.reply(msg, {'type': 'init_ok'})

        self.net.on('init', broadcast_init)

        def broadcast_reply(msg):
            messages = msg['body']['message']
            if isinstance(messages, int):
                messages = [messages]
            for message in messages:
                if message not in self.stored_broadcasts:
                    self.stored_broadcasts.add(message)
                    if 'n' not in msg['src']:
                        self.send_buffer.append((msg['src'], message))
                        
            self.net.reply(msg, {'type': 'broadcast_ok'})
        
        self.net.on('broadcast', broadcast_reply)

        def read_reply(msg):
            response = {'type': 'read_ok', 'messages': list(self.stored_broadcasts)}
            self.net.reply(msg, response)

        self.net.on('read', read_reply)

        def topology_reply(msg):
            response = {'type': 'topology_ok'}
            self.topology = msg['body']['topology']
            self.net.reply(msg, response)
            print(self.topology, file=sys.stderr)
        
        self.net.on('topology', topology_reply)

    def try_retry(self):
        return self.net.retry_all()

    def flush_buffer(self):
        messages = [x[1] for x in self.send_buffer]
        if not messages:
            return
        for neighbour in self.topology:
            if neighbour == self.node_id:
                continue
            self.net.rpc(neighbour, {'type': 'broadcast', 'message': messages}, lambda _: None)
        self.send_buffer = [] 
            

    def try_flush_buffer(self):
        if time.time() - self.last_flushed > 1:
            self.flush_buffer()
            self.last_flushed = time.time()
            return True

    def main(self):
        while True:
            self.net.process_msg() or self.try_retry() or self.try_flush_buffer()
            time.sleep(0.001)

if __name__ == '__main__':
    node = BroadcastNode()
    node.main()