
import os
import sys
import datetime
import functools

import requests

import luigi
from luigi.contrib.ftp import RemoteTarget as FtpRemoteTarget

GEO_PLATFORMS = 'platforms'
GEO_DATASETS = 'datasets'
GEO_SAMPLES = 'samples'
GEO_SERIES = 'series'

KEY_COLLECTION = 'collection'
KEY_COLL_PREFIX = 'coll_prefix'
KEY_TARGET = 'target'
KEY_TARGET_SERIES = 'targ_series'
KEY_REMOTE_FORMAT = 'coll_fmt'
KEY_FORMAT_SUFFIX = 'fmt_suffix'

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

DEFAULT_DOWNLOAD_HOST = 'ftp.ncbi.nlm.nih.gov'

prefixes_per_collection = {
    GEO_PLATFORMS: 'GPL',
    GEO_DATASETS: 'GDS',
    GEO_SAMPLES: 'GSM',
    GEO_SERIES: 'GSE'
}

formats_per_collection = {
    GEO_PLATFORMS: '',
    GEO_DATASETS: 'soft',
    GEO_SAMPLES: '',
    GEO_SERIES: ''
}

def build_download_url(target, path_data=None, collection=None):
    path_dict = path_data or build_path_dict(target=target, collection=collection)
    def bracewrap(s): return '{lbr}{st}{rbr}'.format(lbr='{', rbr='}', st=s)
    format_string = ('geo/'
                     +'/'.join(tuple(map(bracewrap, [
                                      KEY_COLLECTION, 
                                      KEY_COLL_PREFIX+'}{'+KEY_TARGET_SERIES, 
                                      KEY_TARGET,
                                      KEY_REMOTE_FORMAT,
                                      KEY_TARGET
                                    ])
                                ))
                     + '.'
                     + ''.join(tuple(map(bracewrap, [KEY_REMOTE_FORMAT, KEY_FORMAT_SUFFIX])))
                    )
    path_string = format_string.format(**path_dict)
    print(path_string)
    return path_string
    
@functools.lru_cache(maxsize=10)
def build_path_dict(target, collection=None):
    coll = (collection or '').strip().lower()
    
    if not coll:
        for colltype, prefix in prefixes_per_collection.items():
            if prefix in target:
                coll = colltype
                break
    
    prefix = prefixes_per_collection[coll]
    coll_fmt = formats_per_collection[coll]
    
    path_dict = {
        KEY_COLLECTION: coll,
        KEY_COLL_PREFIX: prefix,
        KEY_TARGET_SERIES: target[:4].lstrip(prefix) + 'nnn',
        KEY_TARGET: target,
        KEY_REMOTE_FORMAT: coll_fmt,
        KEY_FORMAT_SUFFIX: '.gz'
    }
    
    return path_dict

class DownloadTask(luigi.Task):
    target_code = luigi.Parameter()
    retry_count = 1
    
    def run(self):
        target = (self.target_code or '').strip()
        path_data = build_path_dict(target=target)
        
        remote = FtpRemoteTarget(
            host=DEFAULT_DOWNLOAD_HOST, 
            path=build_download_url(target=self.target_code, path_data=path_data), 
            format=luigi.format.Gzip
        )
        
        remote.get(self.output().path)
        
    def output(self):
        run_time = datetime.datetime.now()
        run_timestamp = run_time.strftime("%Y-%m-%d")
        
        target = (self.target_code or '').strip()
        path_data = build_path_dict(target=target)
        
        output_name = os.path.join(*map(str, (PROJECT_DIR, 'outputs', run_timestamp, (self.__class__.__name__), target)))
        return luigi.LocalTarget(output_name+path_data[KEY_FORMAT_SUFFIX])
    
def main():
    luigi.build([DownloadTask(target_code='GDS5260')], local_scheduler=True)
    
if __name__ == '__main__':
    main()