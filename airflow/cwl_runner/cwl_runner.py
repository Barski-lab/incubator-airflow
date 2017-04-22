#!/usr/bin/env python
import sys

from airflow.cwl_runner import main

if __name__ == "__main__":
    sys.exit(main.main(sys.argv[1:]))
