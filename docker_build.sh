#!/bin/bash

#static 3MB build, no distro, no shell :)
docker build . -t urtho/algostreamer:rmq
docker push urtho/algostreamer:rmq
