import sys, os, runpy
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "KoreDataGateway"))
runpy.run_path(os.path.join(os.path.dirname(__file__), "KoreDataGateway", "main.py"), run_name="__main__")
