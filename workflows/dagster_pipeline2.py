

from datetime import time
from genericpath import isfile
from dagster import (
    pipeline, solid, composite_solid,
    Output, OutputDefinition,
    DynamicOutputDefinition,
    InputDefinition, ModeDefinition,
    default_executors, default_intermediate_storage_defs,
    resource, Field, repository,
    execute_pipeline
)
import os
import sys
import time
import fire
from dagster.core.definitions.events import DynamicOutput

import skimage.io as skio
import numpy as np
# import matplotlib.pyplot as plt
import pandas as pd
from typing import List as L, Dict as D, Optional as O, Union as U



@solid(
    tags={'dagster-celery/queue': 'cpu'}
)
def get_files_paths(context) -> L[dict]:
    path_idx = context.solid_config['path_idx']
    if not os.path.isfile(path_idx):
        raise FileNotFoundError(f'cant find idx-file ({path_idx})')
    wdir = os.path.dirname(path_idx)
    df = pd.read_csv(path_idx)
    df['path_abs'] = [os.path.join(wdir, x) for x in df['path']]
    ret = [{
            'dir': wdir,
            'path': x,
            'task_key': f'subtask_{xi + 10:010d}'
        } for xi, x in enumerate(df['path_abs']) if os.path.isfile(x)]
    context.log.info(f'!!! #files-all/#files-ok = {len(df)}/{len(ret)}')
    return ret


@solid(
    output_defs=[DynamicOutputDefinition(dict)],
    tags={'dagster-celery/queue': 'cpu'}
)
def generate_subtasks(context, data: L[dict]):
    context.log.info(f'!!! #input-data: {len(data)}')
    for xi, x in enumerate(data):
        yield DynamicOutput(x, mapping_key=x['task_key'])


@solid(
    tags={'dagster-celery/queue': 'cpu'}
)
def process_image(context, sample: dict, sleep_sec: float = 0.1) -> dict:
    time.sleep(sleep_sec)
    path_img = sample['path']
    img = skio.imread(path_img)
    context.log.info(f'::: process() -> {path_img} ')
    ret = float(img.mean())
    return {
        # 'info': sample,
        'mean': float(ret)
    }


@solid(
    tags={'dagster-celery/queue': 'cpu'}
)
def process_scalar(context, sample: dict, sleep_sec: float = 0.1) -> dict:
    time.sleep(sleep_sec)
    mean_value = 10. * sample['mean']
    return {
        'mean': float(mean_value)
    }


@composite_solid(
    input_defs=[InputDefinition("sample", dict)],
    # tags={'dagster-celery/queue': 'cpu'}
)
def process_image_2stage(sample: dict) -> dict:
    return process_scalar(process_image(sample))



@solid(
    tags={'dagster-celery/queue': 'gpu'}
)
def reduce_results(context, data: L[dict]) -> dict:
    context.log.info(f'!!! #data({len(data)}) = {data}')
    mean_images = float(np.mean([x['mean'] for x in data if x is not None]))
    #
    context.log.info(f'!!! reduce-results: {mean_images:0.3f}')
    return {
        'value': mean_images
    }


@pipeline
def map_pipeline():
    files_info = get_files_paths()
    ret_map = generate_subtasks(files_info).map(process_image)
    ret = reduce_results(ret_map.collect())
    # ret = ret_map
    return ret


@pipeline
def map_pipeline2():
    files_info = get_files_paths()
    ret_map = generate_subtasks(files_info).map(process_image_2stage)
    ret = reduce_results(ret_map.collect())
    # ret = ret_map
    return ret


@pipeline
def solid_pipeline():
    files_info = get_files_paths()
    return files_info


@pipeline
def solid_pipeline_2():
    files_info = get_files_paths()
    return files_info


@pipeline
def solid_pipeline_3():
    files_info = get_files_paths()
    return files_info


@repository
def map_pipeline_repo():
    return [map_pipeline, map_pipeline2, solid_pipeline, solid_pipeline_2, solid_pipeline_3]


def main(
    path_idx: str = '/home/ar/github.com_ext/dagster-celery-docker-example/data/idx-mitosis.txt'
):
    run_config = {
        "solids": {
            "get_files_paths": {
                "config": {"path_idx": path_idx}
            }
        }
    }
    print(f'run_config = {run_config}')
    # ret = execute_pipeline(map_pipeline, run_config=run_config)
    ret = execute_pipeline(map_pipeline2, run_config=run_config)
    print(f'!!! @ret = {ret}')


if __name__ == '__main__':
    fire.Fire(main)
