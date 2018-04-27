#!/usr/bin/env python2.7
from __future__ import print_function

import os
import time
import shlex
import subprocess
import json
import socket
from argparse import ArgumentParser

from common_utils.s3_utils import download_file,upload_folder,get_size,get_aws_session

def fixResolv():
    """
    Fix issue with gethostname() in MPI calls
    """
    with open("/etc/hosts", "a") as hostsfile:
       hostsfile.write('127.0.0.1 %s' % (socket.gethostname()) )

def run_vcf2tiledb_basic(idx, loader_path):
    """
    Runs vcf2tiledb
    :param loader_path: s3 path to loader.json
    :param idx: index in loader
    """
    cred = get_aws_session()
    os.environ['AWS_ACCESS_KEY_ID'] = cred.access_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = cred.secret_key
    os.environ['AWS_SESSION_TOKEN'] = cred.token

    fixResolv()

    cmd = 'vcf2tiledb -r%d %s' % (idx, loader_path)

    print('Running cmd:= ', end='')
    print(cmd)

    return subprocess.check_call(shlex.split(cmd))


def main():
    argparser = ArgumentParser()

    file_path_group = argparser.add_argument_group(title='File paths')
    file_path_group.add_argument('--loader_s3_path', type=str, help='loader.json s3 path', required=True)
    file_path_group.add_argument('--callset_s3_path', type=str, help='callset.json s3 path', required=True)
    file_path_group.add_argument('--results_s3_path', type=str, help='results s3 path', required=True)
    file_path_group.add_argument('--vid_s3_path', type=str, help='VID s3 path', required=True)
    file_path_group.add_argument('--index', type=int, help='index in loader', required=True)

    args = argparser.parse_args()
    print(args)

    print("Downloading loader file")
    loader_path = download_file(args.loader_s3_path, '/')
    print("loader file downloaded to %s" % loader_path)

    print("Downloading callset file")
    callset_path = download_file(args.callset_s3_path, '/')
    print("callset downloaded to %s" % callset_path)

    print("Downloading VID file")
    vid_path = download_file(args.vid_s3_path, '/')
    print("VID downloaded to %s" % vid_path)


    # Calculate required result output based on input files in callset.json
    total_size = 0
    with open(callset_path, "r") as text_file:
       callset_array = json.load(text_file)

    for sample, value in callset_array['callsets'].items():
        total_size += get_size(value['filename'])

    print("Total Size := {0}".format(total_size) )
    del callset_array

    # Declare expected disk usage, triggers host's EBS script (ecs-ebs-manager)
    with open("/TOTAL_SIZE", "w") as text_file:
       text_file.write("{0}".format(total_size))

    # Wait for EBS to appear
    while not os.path.isdir('/scratch'):
       time.sleep(5)

    # Wait for mount verification
    while not os.path.ismount('/scratch'):
       time.sleep(1)

    if not os.path.exists('/scratch/pVCF_genomicsDB'):
       os.mkdir('/scratch/pVCF_genomicsDB')

    print ("Running vcf2tiledb_basic")
    local_stats_path = run_vcf2tiledb_basic(
                           args.index,
                           loader_path)

    print("Uploading to %s" % (args.results_s3_path) )
    upload_folder(args.results_s3_path, '/scratch/pVCF_genomicsDB')

    print ("Completed")


if __name__ == '__main__':
    main()
