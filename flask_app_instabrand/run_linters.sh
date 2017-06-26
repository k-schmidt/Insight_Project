#! /bin/bash

mypy --ignore-missing-imports run.py
pylint run.py
