# AlgoNode algostreamer utility

## About algostreamer

Small utility to stream past and/or current Algorand node JSON blocks to Redis or stdout.
Blocks and TXn can be filtered and transformed and redirected using REGO - an Open Policy Agent language.

## About AlgoNode

We are here to help your Algorand node shine!
Go to https://algonode.cloud for more.

## Install 

```Shell
go get github.com/algonode/algostreamer
```

## Config

config.json
```json
{
  "algod" : {
    "address" : "http://localhost:8080",
    "token" : "...",
    "queue" : 100
  },
  "redis": {
    "addr": "localhost",
    "user": "",
    "pass": "",
    "db": 10
  }
}
```

* You can find your token in node/data/algo.token
* You can find your address in node/data/alogo.net

## Run

Start streaming from the current block
```Shell
./algostreamer -f config.jsonc 
```

Start streming from the block no 18000000 and then continue with current blocks
```Shell
./algostreamer -r 18000000 -f config.jsonc
```

## License

Copyright (C) 2022 AlgoNode Org.

algostreamer is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

## Support AlgoNode

if you like what we do feel free to support us by sending some microAlgos to

**AlgoNode wallet**: `S322JRT4RZ4L2CLEQ5HXQBU2CNH3DLLO6JJEMWGPQHAOG2ALCH7ZAHXOPE`
