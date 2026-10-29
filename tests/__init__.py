import os
import sys

SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
for path in (SRC, os.path.join(SRC, "client"), os.path.join(SRC, "gateway"), os.path.join(SRC, "nodes", "classifier"),
             os.path.join(SRC, "nodes", "text_processor"), os.path.join(SRC, "nodes", "word_counter"),
             os.path.join(SRC, "nodes", "join_q3"), os.path.join(SRC, "nodes", "top20"),
             os.path.join(SRC, "nodes", "filter_biography"), os.path.join(SRC, "nodes", "image_downloader"),
             os.path.join(SRC, "nodes", "average"), os.path.join(SRC, "nodes", "comparator")):
    if path not in sys.path:
        sys.path.insert(0, path)
