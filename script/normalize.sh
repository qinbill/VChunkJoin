#!/bin/bash

sed -e 's/\(.*\)/\L\1/' | sed 's/[^a-zA-Z]/_/g'
