#!/bin/bash

export CURRENCY_ONE=AA0
export CURRENCY_TWO=BB0

for i in {0..2}
do
  export PORT=666$i
  node retriever.js 2>&1 >/dev/null &
done

