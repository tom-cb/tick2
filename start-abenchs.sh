#!/bin/bash

for i in {0..2}
do
 ab -n 1000 -c 1 http://127.0.0.1:666$i/  > ~/rough/ab$i 2>&1 &
done

