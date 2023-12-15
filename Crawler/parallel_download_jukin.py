from argparse import ArgumentParser
import os
import numpy as np
import json as js
import gcsfs
import fsspec
from concurrent.futures import ThreadPoolExecutor
import time
import math
import sys
import time
import ffmpeg
from concurrent.futures import ThreadPoolExecutor
import threading
from pathlib import Path
import requests
import tempfile

TMP_DIR = '/mnt/ramdisk'

class ThreadPool:
    def __init__(self, max_thread_num=5):
        self.over = False
        self.results = []
 
        self.func = None
        self.args_list = None
        self.task_num = 0
        self.max_thread_num = max_thread_num
        self.pool = ThreadPoolExecutor(max_workers=max_thread_num)
        self.cond = threading.Condition()
 
    def set_tasks(self, func, args_list):
        self.task_num = len(args_list)
        self.args_list = args_list
        self.func = func
 
    @staticmethod
    def show_process(desc_text, curr, total):
        proc = math.ceil(curr / total * 100)
        show_line = '\r' + desc_text + ':' + '>' * proc \
                    + ' ' * (100 - proc) + '[%s%%]' % proc \
                    + '[%s/%s]' % (curr, total)
        sys.stdout.write(show_line)
        sys.stdout.flush()
        time.sleep(0.1)
 
    def get_result(self, future):
        self.show_process('Progress', self.task_num - len(self.args_list), self.task_num)
        self.results.append(future.result())
        if len(self.args_list):
            args = self.args_list.pop()
            task = self.pool.submit(self.func, *args)
            task.add_done_callback(self.get_result)
        else:
            if self.task_num == len(self.results):
                print('\n', 'Done')
                self.cond.acquire()
                self.cond.notify()
                self.cond.release()
            return
 
    def _start_tasks(self):
        for i in range(self.max_thread_num):
            if len(self.args_list):
                args = self.args_list.pop()
                task = self.pool.submit(self.func, *args)
                task.add_done_callback(self.get_result)
            else:
                break
 
    def final_results(self):
        self._start_tasks()
        if self.task_num == len(self.results):
            return self.results
        else:
            self.cond.acquire()
            self.cond.wait()
            self.cond.release()
            return self.results

def download_process_save(save_dir, jmId):
    fs = gcsfs.GCSFileSystem()
    fsspec.asyn.iothread[0] = None
    fsspec.asyn.loop[0] = None

    headers = {
    "X-Algolia-Api-Key": "a6099f9d3771d6ceb142321ac5273d16",
    "X-Algolia-Application-Id": "XSWHBQ6C6E",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }
    input_fname = f"{TMP_DIR}/v_{jmId}.mp4"
    output_fname = f"{TMP_DIR}/v_{jmId}_2.mp4"
    try:
        url = 'https://www.jukinmedia.com/api/public/video/downloadVideo/'+jmId
        response = requests.post(url,headers=headers)
        video_response = requests.get(js.loads(response.content)['url'])
        video_bytes = video_response.content
        with open(input_fname, "wb") as f:
            f.write(video_bytes)

        fps = 4
        image_size = 256
        (
            ffmpeg.input(input_fname)
            .filter("fps", fps=fps, round='up')
            .filter("scale", w=image_size, h=image_size, force_original_aspect_ratio="increase")
            .filter("crop", image_size, image_size)
            .output(output_fname)
            .run(capture_stdout=True, capture_stderr=True)
        )

        fs.put_file(output_fname, f"{save_dir}/v_{jmId}.mp4")
        os.system(f"rm {input_fname}")
        os.system(f"rm {output_fname}")
        print('{} succeed!'.format(jmId))
    except Exception as e:
        print(f'{url} error:', e)


def check_already(save_dir, args_list):
    fs = gcsfs.GCSFileSystem()
    fsspec.asyn.iothread[0] = None
    fsspec.asyn.loop[0] = None

    files = fs.glob(f"{save_dir}/*.mp4")
    jm_ids = list(set([Path(f).stem.split('_')[1] for f in files]))
    result = []
    for _, arg in args_list:
        if arg not in jm_ids:
            result.append((_,arg))
    print('already {}, left {}'.format(len(jm_ids), len(result)))
    return result


def main(args):
    input_file_path = Path(args.input_file)
    all_data = js.load(open(input_file_path,'r'))
 
#    Path(args.save_dir).mkdir(exist_ok=True, parents=True)
    tp = ThreadPool(args.num_process)
    args_list = []
    for cat in all_data:
        args_list+=all_data[cat]
    args_list = [(args.save_dir,data['jmId']) for data in args_list]
    args_list = check_already(args.save_dir,args_list)
    tp.set_tasks(download_process_save, args_list)
    res = tp.final_results()
    
if __name__ == "__main__":
    parser = ArgumentParser(description="Script to parallel downloads videos")
    parser.add_argument("--save_dir", default="gs://rll-tpus-wilson-us-central2/datasets/jukin")
    parser.add_argument("--input_file", default='jukin-100k.json')
    parser.add_argument("--num_process", type = int, default=8)
    args = parser.parse_args()
    main(args)
