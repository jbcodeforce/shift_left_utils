import os
import pytest

def test_os_walk():
    for root, dirs, files in os.walk(os.getenv("PIPELINES") + "/facts"):
        print(root)
        print(dirs)
        if "pipeline_definition.json" in files:
            print(files)
        dirname = os.path.dirname(root).split("/")[-1]
        print(dirname)
                

    
    