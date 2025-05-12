#!/bin/bash

# Exit if any command fails
set -e

echo "Creating virtual environment..."
python3 -m venv venv

echo "Activating virtual environment..."
source venv/bin/activate

echo "Upgrading pip..."
pip install --upgrade pip

echo "Installing dependencies from requirements.txt..."
pip install -r requirements.txt

echo "Setup complete. To activate later, run:"
echo "  source venv/bin/activate"
