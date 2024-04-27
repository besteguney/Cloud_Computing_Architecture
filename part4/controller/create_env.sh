#!/bin/bash

# Create virtual environment
echo "Setting up virtual environment and installing packages"
python3 -m venv myenv

# Activate virtual environment
echo "Activating virtual environment"
source myenv/bin/activate      # On Linux/Mac

echo "Installing the packages"
pip install -r requirements.txt

if [ -n "$VIRTUAL_ENV" ]; then
    echo "Virtual environment is active."
else
    echo "Virtual environment is not active."
fi