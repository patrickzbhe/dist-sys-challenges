import json
import sys
import select
import time


class RpcHandler:
    def __init__(self, handler, body, dest):
        self.handler = handler
        self.body = body
        self.dest = dest
        self.created_at = time.time()


class Net:
    def __init__(self, retry_delay=0.3):
        self.node_id = None
        self.next_msg_id = 0
        self.handlers = {}
        self.callbacks = {}
        self.sent_dests = set()
        self.retry_delay = retry_delay

    def new_msg_id(self):
        id = self.next_msg_id
        self.next_msg_id += 1
        return id

    def on(self, msg_type, handler):
        if msg_type in self.handlers:
            raise ValueError("Duplicate handler for message type")
        self.handlers[msg_type] = handler

    def send_msg(self, msg):
        json.dump(msg, sys.stdout)
        sys.stdout.write("\n")
        sys.stdout.flush()

    def send(self, dest, body):
        self.send_msg({"src": self.node_id, "dest": dest, "body": body})

    def reply(self, req, body):
        body["in_reply_to"] = req["body"]["msg_id"]
        self.send(req["src"], body)

    def rpc(self, dest, body, handler):
        msg_id = self.new_msg_id()
        body["msg_id"] = msg_id
        body = body.copy()
        self.callbacks[msg_id] = RpcHandler(handler, body, dest)
        self.sent_dests.add(dest)
        self.send(dest, body)

    def retry(self, msg_id):
        handler = self.callbacks[msg_id]
        if time.time() - handler.created_at < self.retry_delay:
            return False
        print("retrying...", file=sys.stderr)
        new_msg_id = self.new_msg_id()
        body = handler.body.copy()
        body["msg_id"] = new_msg_id
        self.callbacks[new_msg_id] = RpcHandler(handler.handler, body, handler.dest)
        self.send(handler.dest, body)
        del self.callbacks[msg_id]
        return True

    def retry_all(self):
        keys = list(self.callbacks.keys())
        retried = False
        for msg_id in keys:
            retried |= self.retry(msg_id)
        return retried

    def process_msg(self):
        if sys.stdin not in select.select([sys.stdin], [], [], 0)[0]:
            return None

        line = sys.stdin.readline()

        if not line:
            return None

        msg = json.loads(line)
        body = msg["body"]

        handler = None
        if "in_reply_to" in body:
            m = body["in_reply_to"]
            if m not in self.callbacks:
                return True
            handler = self.callbacks[m].handler
            del self.callbacks[m]

        elif body["type"] in self.handlers:
            handler = self.handlers[body["type"]]

        else:
            raise RuntimeError("No callbacks or handlers defined for this message")

        handler(msg)
        return True
