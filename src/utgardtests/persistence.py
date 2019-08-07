"""Functions for saving and reading test data and results.

In the save functions, the dict arguments are traversed recursively, with a
folder created for each key holding a dict as its value, and a file is written
for each object held as a value.
"""

import os
import queue
import matplotlib
import pandas


def save_run(
    path: str, log: queue.Queue = None, errors: dict = None, data: dict = None
):
    os.mkdir(path)

    if log is not None:
        _write_log_to_disk(os.path.join(path, "log"), log)

    if errors is not None:
        errors_dir = os.path.join(path, "errors")
        os.mkdir(errors_dir)
        _write_errors_to_disk(errors_dir, errors)

    if data is not None:
        data_dir = os.path.join(path, "data")
        os.mkdir(data_dir)
        _write_data_to_disk(data_dir, data)


def save_results(path: str, results: dict):
    os.makedirs(path, exist_ok=True)
    results_dir = os.path.join(path, "results")
    os.mkdir(results_dir)
    _write_data_to_disk(results_dir, results)


def save_figures(path: str, figures: dict):
    os.makedirs(path, exist_ok=True)
    figures_dir = os.path.join(path, "figures")
    os.mkdir(figures_dir)
    _write_figures_to_disk(figures_dir, figures)


def save_info(path: str, info: str):
    with open(path, "w") as f:
        f.write(info)


def read_run(path: str) -> (queue.Queue, dict, dict):
    log = _read_log_from_disk(os.path.join(path, "log"))
    errors = _read_errors_from_disk(os.path.join(path, "errors"))
    data = _read_data_from_disk(os.path.join(path, "data"))
    return log, errors, data


def read_info(path: str) -> str:
    with open(path, "r") as f:
        lines = f.readlines()

    return "".join(lines)


def _write_log_to_disk(path, log):
    with open(path, "w") as f:
        while not log.empty():
            f.write("{}\n".format(log.get()))


def _write_errors_to_disk(path, errors):
    for key in errors:
        value = errors[key]
        dest = os.path.join(path, key)
        if isinstance(value, list):
            with open(dest, "w") as f:
                for v in value:
                    f.write("{}\n".format(v))
        else:
            os.mkdir(dest)
            _write_errors_to_disk(dest, value)


def _write_data_to_disk(path, data):
    for key in data:
        value = data[key]
        dest = os.path.join(path, key)
        if isinstance(value, pandas.DataFrame):
            value.to_csv(dest + ".csv")
        else:
            os.mkdir(dest)
            _write_data_to_disk(dest, value)


def _write_figures_to_disk(path, figures):
    for key in figures:
        value = figures[key]
        dest = os.path.join(path, key)
        if isinstance(value, matplotlib.figure.Figure):
            value.savefig(dest + ".pdf", bbox_inches="tight")
        else:
            os.mkdir(dest)
            _write_figures_to_disk(dest, value)


def _read_log_from_disk(path):
    log = queue.Queue()
    with open(path, "r") as f:
        lines = f.readlines()

    for line in lines:
        log.put(line.strip("\n"))

    return log


def _read_errors_from_disk(path):
    if not os.path.isdir(path):
        with open(path, "r") as f:
            errors = f.readlines()
        return errors
    else:
        sub_dirs = os.listdir(path)
        errors = {}
        for p in sub_dirs:
            errors[p] = _read_errors_from_disk(os.path.join(path, p))
        return errors


def _read_data_from_disk(path):
    if not os.path.isdir(path):
        return pandas.read_csv(path, index_col=0)
    else:
        sub_dirs = os.listdir(path)
        data = {}
        for p in sub_dirs:
            key = os.path.splitext(p)[0]
            data[key] = _read_data_from_disk(os.path.join(path, p))
        return data
