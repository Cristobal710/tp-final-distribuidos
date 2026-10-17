import os
import sys

SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
for path in (SRC, os.path.join(SRC, "client")):
    if path not in sys.path:
        sys.path.insert(0, path)
