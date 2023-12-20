import os, sys


sys.path.append(
    os.path.join("/", 
        *os.path.dirname(os.path.realpath(__file__)).split("/")[:-1], "app"
    )
)