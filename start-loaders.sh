#!/bin/bash

export CURRENCY_ONE=AA
export CURRENCY_TWO=BB

node loader.js 2>&1 >/dev/null &

sleep 20;

export CURRENCY_ONE=CC
export CURRENCY_TWO=DD

node loader.js 2>&1 >/dev/null &

sleep 20;

export CURRENCY_ONE=EE
export CURRENCY_TWO=FF

node loader.js 2>&1 >/dev/null &

sleep 20;

export CURRENCY_ONE=GG
export CURRENCY_TWO=HH

node loader.js 2>&1 >/dev/null &

