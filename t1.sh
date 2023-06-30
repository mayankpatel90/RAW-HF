#!/bin/sh
sudo iotop -b -o -d 1 -a | grep Total;
