#!/bin/bash

if [[ -z "$VIRTUAL_ENV" ]]; then
    echo "error: no virtualenv is active. activate one first."
    exit 1
fi

arg="tests/"
if [[ -n "$@" ]]; then
    arg="$@"
fi

py.test -x --cov=aiorethink --cov-report html --cov-report term --durations=10 $arg
