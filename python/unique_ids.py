#!/usr/bin/env python3
from node import Net
import time
import uuid

class UUIDNode:
    def __init__(self):
        self.net = Net()
        self.node_id = None
        self.setup_handlers()

    def set_node_id(self, id):
        self.node_id = id
        self.net.node_id = id

    def setup_handlers(self):
        def echo_init(msg):
            body = msg['body']
            self.set_node_id(body['node_id'])
            self.net.reply(msg, {'type': 'init_ok'})

        self.net.on('init', echo_init)

        def echo_reply(msg):
            response = msg['body'].copy()
            response['type'] = 'generate_ok'
            response['id'] = str(uuid.uuid1())
            self.net.reply(msg, response)
        
        self.net.on('generate', echo_reply)
    
    def main(self):
        while True:
            self.net.process_msg()
            time.sleep(0.001)

if __name__ == '__main__':
    node = UUIDNode()
    node.main()