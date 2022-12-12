import os
import importlib


def module_load(base_path):
    modules = {}

    bootstrap_file_path = os.path.join(
        base_path, "module",
        "bootstrap.py")
    if os.path.isfile(bootstrap_file_path):
        mod = importlib.import_module(
            f"module")
        c = mod.get_module_class()
        module_info = c.get_module_info()
        modules[module_info.name] = c

    return modules
