#!/usr/bin/env python2.7
from __future__ import print_function

import os
import time
import json
from argparse import ArgumentParser
from common_utils.genomicsdb import run_vcf2tiledb_no_s3
from common_utils.s3_utils import download_file,upload_folder
import common_utils.ebs_utils

WORKDIR = '/scratch'
PVCFDIR = WORKDIR + '/pVCF_genomicsDB'

##
# ENV: DEBUG, GETEBS, EBSSIZE

def parse4vcf2tiledb(argparser = ArgumentParser()):
    file_path_group = argparser.add_argument_group(title='File paths')
    file_path_group.add_argument('--loader_s3_path', type=str, help='loader.json s3 path', required=True)
    file_path_group.add_argument('--callset_s3_path', type=str, help='callset.json s3 path', required=True)
    file_path_group.add_argument('--results_s3_path', type=str, help='results s3 path', required=True)
    file_path_group.add_argument('--vid_s3_path', type=str, help='VID s3 path', required=True)

    genomicsdb_group = argparser.add_argument_group(title='File paths')
    genomicsdb_group.add_argument('--chr', type=str, help='chromosome, e.g. chr1', required=True)
    genomicsdb_group.add_argument('--index', type=int, help='index in loader', required=False)

    return argparser

def download_required_files(*args):

    fList = []
    for f in args:
        print("Downloading {}".format(f))
        downloaded_path = download_file(f, '/')
        print("file downloaded to {}" % (downloaded_path))
        fList.append(downloaded_path)

    return fList

def main():
    argparser = parse4vcf2tiledb()
    args, extr = argparser.parse_known_args()
    print(args)

    if args.index == None:
      idx = int(os.getenv('AWS_BATCH_JOB_ARRAY_INDEX'))
    else:
      idx = args.index

    loader_path, callset_path, vid_path = download_required_files(args.loader_s3_path, args.callset_s3_path, args.vid_s3_path)

    if os.getenv('GETEBS'):
        ebs_utils.initEBS()
    else:
        if not os.path.exists(WORKDIR):
            os.mkdir(WORKDIR)

    if not os.path.exists(PVCFDIR):
       os.mkdir(PVCFDIR)

    # Run program
    run_vcf2tiledb_no_s3(WORKDIR, idx, loader_path, callset_path, vid_path, args.chr)

    if not os.getenv('SKIP_UPLOAD'):
      print("Uploading to %s" % (args.results_s3_path) )
      upload_folder(args.results_s3_path, PVCFDIR)

    print ("Completed")


if __name__ == '__main__':
    main()
