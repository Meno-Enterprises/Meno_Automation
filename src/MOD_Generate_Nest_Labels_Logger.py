#!/usr/bin/env python3

import logging

# Configure the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', handlers=[
    logging.FileHandler('logs/MOD_Generate_Nest_Labels.log'),
    logging.StreamHandler()
])

# Create a logger instance
logger = logging.getLogger('shared_logger')