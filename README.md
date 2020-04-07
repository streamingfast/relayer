## Goal

Read blocks from mindreader (which has a WS endpoint, pushing blocks
blindly), and fan out to more websocket subscribers, with deduping and
buffering for quick ramp-up of `eosws`  (or other) consuming nodes. the
deduping+buffering should be done with a forkdb to prevent weird races

## depends on
* bstream websocket source to different mindreader endpoints
* bstream multiplexedSource to manage them
* bstream joiner (eternal) to get blocks from files and quickly find a valid longest chain up to lib
* bstream forkable + filter on "new" blocks only
* bstream blockServer to serve blocks (with a buffer)
