#!/bin/bash
source $HOME/.bashrc

python_script="${1:-importer.py}"

python3 submit.py "$python_script"