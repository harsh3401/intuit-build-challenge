#!/usr/bin/env bash
set -e

VENV_NAME="venv"
REQUIRED_PYTHON="3"

echo "Checking Python 3 availability..."
python3 --version

echo "Creating virtual environment with python3..."
python3 -m venv $VENV_NAME

echo "Activating virtual environment..."
source $VENV_NAME/bin/activate

echo "Upgrading pip..."
pip install --upgrade pip

if [ -f requirements.txt ]; then
    echo "Installing dependencies..."
    pip install -r requirements.txt
fi

echo "Environment setup complete."
python --version
