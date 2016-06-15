#!/bin/bash

echo "Started testing Python script";

python test_python.py a b "A String" D &

v_pid=$!;

if wait $v_pid; then echo "Python Script executed without errors"; else echo "Python Script failed"; fi

echo $(date) "End of testing Python script"



