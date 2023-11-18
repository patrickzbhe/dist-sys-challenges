#!/usr/bin/env python3
from node import Net
import time
import sys


class CounterNode:
    def __init__(self):
        self.net = Net()
        self.node_id = None
        self.counter_buffer = 0
        self.counter_total = 0
        self.last_read_value = 0
        self.last_read_time = time.time()
        self.last_write_time = time.time()
        self.MAX_WRITE_TIME = 0.3
        self.MAX_READ_TIME = 0.3
        self.update_lock = False
        self.read_lock = False
        self.setup_handlers()

    def set_node_id(self, id):
        self.node_id = id
        self.net.node_id = id

    def setup_handlers(self):
        def echo_init(msg):
            body = msg["body"]
            self.set_node_id(body["node_id"])
            self.net.reply(msg, {"type": "init_ok"})

        self.net.on("init", echo_init)

        def counter_read(msg):
            response = {}
            response["type"] = "read_ok"
            response["value"] = self.last_read_value + self.counter_buffer
            self.net.reply(msg, response)

        self.net.on("read", counter_read)

        def counter_add(msg):
            response = {}
            response["type"] = "add_ok"
            self.counter_buffer += msg["body"]["delta"]
            self.net.reply(msg, response)

        self.net.on("add", counter_add)

    def update_global_counter(self):
        if self.update_lock:
            return
        cur_buffer = self.counter_buffer
        def update_callback(msg):
            if msg['body']['type'] != 'error':
                self.counter_buffer -= cur_buffer

            self.update_lock = False
        self.update_lock = True
        self.net.rpc(
            "seq-kv",
            {
                "type": "cas",
                "key": "counter",
                "from": self.last_read_value,
                "to": self.last_read_value + self.counter_buffer,
                "create_if_not_exists": True,
            },
            update_callback,
        )

    def try_update_global_counter(self):
        if time.time() - self.last_write_time > self.MAX_WRITE_TIME:
            self.update_global_counter()
            self.last_write_time = time.time()
            return True
        return False

    def read_global_counter(self):
        if self.read_lock:
            return

        def read_callback(msg):
            try:
                self.last_read_value = msg['body']['value']
            except KeyError:
                ...
            self.read_lock = False

        self.read_lock = True
        self.net.rpc(
            "seq-kv",
            {
                "type": "read",
                "key": "counter",
            },
            read_callback,
        )

    def try_read_global_counter(self):
        if time.time() - self.last_read_time > self.MAX_READ_TIME:
            self.read_global_counter()
            self.last_read_time = time.time()
            return True
        return False

    def main(self):
        while True:
            self.net.process_msg() or self.try_update_global_counter() or self.try_read_global_counter()
            time.sleep(0.001)


if __name__ == "__main__":
    node = CounterNode()
    node.main()
