#!/bin/bash

#static 3MB build, no distro, no shell :)
docker build . -t urtho/algostreamer:latest
docker push urtho/algostreamer:latest
