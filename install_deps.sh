#!/bin/bash

python3 -m pip install --requirement <(poetry export --dev --format requirements.txt)
python3 -m pip install --no-deps .


