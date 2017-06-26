#! /bin/bash

mypy --ignore-missing-imports main.py
pylint main.py
