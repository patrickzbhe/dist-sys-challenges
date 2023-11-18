# Gossip Glomers in Python
https://fly.io/dist-sys/

## Solutions
### Challenge 2: Unique ID Generation
Just made each node independantly return UUIDs
Different approaches for sorting, better guarantees etc.

### Challenge 3: Broadcast
Buffered and batch sent to reduce total # of messages.

Not sure if cheating but I only broadcast if src is not from our server network. Prevents exponential broadcast calls. Otherwise, could always just send to some 'designated sender node' like n0, but then there's a single point of failure.

Can easily play around with adjusting buffer wait time to get max latency under certain amount of time while adjusting # total messages.

### Challenge 4: Grow-only Counter
Every node stores a local 'unsent' count, and a 'last read' count indicating the last value it read from the kv store. Every few seconds, do an async read. Also try to do an async flush of the local 'unsent', by doing a compare and swap using the last read count + local buffer. Cas guarantees atomicity.

