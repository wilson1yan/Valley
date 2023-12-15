import argparse
from tqdm import tqdm
import json
import gcsfs
import ffmpeg
import tempfile
import multiprocessing as mp
import requests
import numpy as np
from pathlib import Path
import os
import os.path as osp


TMP_DIR = Path('/mnt/ramdisk')


def download(save_path, jm_id):
    try:
        headers = {
            "X-Algolia-Api-Key": "a6099f9d3771d6ceb142321ac5273d16",
            "X-Algolia-Application-Id": "XSWHBQ6C6E",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        }
        response = requests.post('https://www.jukinmedia.com/api/public/video/downloadVideo/' + jm_id, headers=headers)
        video_response = requests.get(json.loads(response.content)['url'])
        f = save_path.open('wb')
        f.write(video_response.content)
        f.close()
        return True
    except Exception as e:
        print(f'error {jm_id}: {e}')
        return False


def process(src_path, dst_path, fps, image_size):
    stream = (
        ffmpeg.input(str(src_path))
        .filter('fps', fps=fps, round='up')
        .filter('scale', w=image_size, h=image_size, force_original_aspect_ratio='increase')
        .filter('crop', image_size, image_size)
        .output(str(dst_path), crf=18)
        .run(capture_stdout=True, capture_stderr=True, overwrite_output=True)
    )


def worker(jm_ids, args, worker_id):
    n, n_success = 0, 0
    fs = gcsfs.GCSFileSystem()
    output_folder = Path(args.output_folder)
    pbar = tqdm(total=len(jm_ids), disable=worker_id > 0, smoothing=0)
    for jm_id in jm_ids:
        jm_folder = TMP_DIR / jm_id
        jm_folder.mkdir(exist_ok=True)
        success = download(jm_folder / '0.mp4', jm_id)
        n += 1
        if not success:
            os.system(f'rm -rf {jm_folder}')
            continue
        process(jm_folder / '0.mp4', jm_folder / '1.mp4', args.fps, args.resolution)
        fs.put_file(str(jm_folder / '1.mp4'), str(output_folder / f'v_{jm_id}.mp4'))
        os.system(f'rm -rf {jm_folder}')
        n_success += 1
        pbar.update(1)
        pbar.set_description(f"success: {n_success}/{n}={n_success/n:.2f}")
    pbar.close()

def main():
    data = json.load(open(args.data_file, 'r'))
    data = sum([data[cat] for cat in data], [])
    jm_ids = [d['jmId'] for d in data]
    jm_id_chunks = [jm_id_chunk.tolist() for jm_id_chunk in np.array_split(jm_ids, args.num_workers)]
    procs = [mp.Process(target=worker, args=(jm_id_chunks[i], args, i)) for i in range(args.num_workers)]
    [p.start() for p in procs]
    [p.join() for p in procs]
    print('done')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data_file', type=str, default='jukin-100k.json')
    parser.add_argument('-r', '--resolution', type=int, default=400)
    parser.add_argument('-f', '--fps', type=int, default=24)
    parser.add_argument('-w', '--num_workers', type=int, default=16)
    parser.add_argument('-o', '--output_folder', type=str, default='rll-tpus-wilson-us-central2/datasets/valley_jukin')
    args = parser.parse_args()
    mp.set_start_method('spawn')
    main()
