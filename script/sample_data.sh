#!/bin/bash

query_number=$1
awk 'BEGIN{srand(232)}{print rand(),$0}' | sort -k1nr | head -n $query_number | cut -d' ' -f2-



