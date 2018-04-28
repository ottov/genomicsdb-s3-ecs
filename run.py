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

WORKDIR = '.'
PVCFDIR = WORKDIR + '/pVCF_genomicsDB'

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

    try:
        subprocess.check_call(shlex.split(cmd))
        return True
    except subprocess.CalledProcessError, e:
        if e.returncode == -11:
            # Possible connection issue
            print("ReturnCode -11")
            return False
        else:
            raise

def run_vcf2tiledb_no_s3(idx, loader_path, callset_path, vid_path):
    """
    fallback to downloading each file. This method uses
    tabix to read the intervals from each file and save interval vcf
    as a local file.
    """
    print("Performing fallback")

    # find offset for this chr
    chr = os.getenv('CHR')

    with open(vid_path) as vid_file:
        hg = json.load(vid_file)

    offset = hg['contigs'][chr]['tiledb_column_offset']

    # extract start/end for this partition
    with open(loader_path) as loader_file:
        ldr = json.load(loader_file)

    start = ldr['column_partitions'][idx]['begin'] - offset
    end   = ldr['column_partitions'][idx]['end']   - offset
    del ldr

    pos = "%s:%s-%s" % (chr, start, end)

    # Download interval files to workdir
    with open(callset_path) as callset_fp:
        fList = json.load(callset_fp)
        fListNew = fList

    for SM in fList['callsets']:
        #download_file(fList['callsets'][SM]['filename'], WORKDIR)
        s3path = fList['callsets'][SM]['filename']
        fName = os.path.basename(s3path)
        cmd = 'tabix -h %s %s | bgzip > %s/%s' % (s3path, pos, WORKDIR, fName)
        print (cmd)
        subprocess.check_call(cmd, shell=True)

        # Build index for this new file
        cmd = 'tabix -f %s/%s' % (WORKDIR, fName)
        subprocess.check_call(shlex.split(cmd))

        #record new callset location
        fListNew['callsets'][SM]['filename'] = WORKDIR + '/' + fName

    # Re-write callsets to local file
    with open(callset_path, 'w') as callset_fp:
        json.dump(obj=dict(
                        fListNew
                      ),
                  indent=2,
                  separators=(',', ': '), fp=callset_fp)

    # Run cmd
    cmd = 'vcf2tiledb -r%d %s' % (idx, loader_path)

    print('Running cmd:= ', end='')
    print(cmd)

def main():
    argparser = ArgumentParser()

    file_path_group = argparser.add_argument_group(title='File paths')
    file_path_group.add_argument('--loader_s3_path', type=str, help='loader.json s3 path', required=True)
    file_path_group.add_argument('--callset_s3_path', type=str, help='callset.json s3 path', required=True)
    file_path_group.add_argument('--results_s3_path', type=str, help='results s3 path', required=True)
    file_path_group.add_argument('--vid_s3_path', type=str, help='VID s3 path', required=True)
    file_path_group.add_argument('--index', type=int, help='index in loader', required=False)

    args = argparser.parse_args()
    print(args)
    if os.getenv('DEBUG'):
        print('Entering sleep')
        time.sleep(9999)

    if args.index == None:
      idx = int(os.getenv('AWS_BATCH_JOB_ARRAY_INDEX'))
    else:
      idx = args.index

    print("Downloading loader file")
    loader_path = download_file(args.loader_s3_path, '/')
    print("loader file downloaded to %s" % loader_path)

    print("Downloading callset file")
    callset_path = download_file(args.callset_s3_path, '/')
    print("callset downloaded to %s" % callset_path)

    print("Downloading VID file")
    vid_path = download_file(args.vid_s3_path, '/')
    print("VID downloaded to %s" % vid_path)

    if os.getenv('GETEBS'):
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
        print('Wait EBS')
        while not os.path.isdir(WORKDIR):
            time.sleep(5)

        # Wait for mount verification
        while not os.path.ismount(WORKDIR):
            time.sleep(1)

        print('EBS found')
    else:
        if not os.path.exists(WORKDIR):
            os.mkdir(WORKDIR)

    if not os.path.exists(PVCFDIR):
       os.mkdir(PVCFDIR)

    print ("Running vcf2tiledb_basic")
    if not run_vcf2tiledb_basic(idx, loader_path):
        run_vcf2tiledb_no_s3(idx, loader_path, callset_path, vid_path)

    print("Uploading to %s" % (args.results_s3_path) )
    upload_folder(args.results_s3_path, PVCFDIR)

    print ("Completed")


if __name__ == '__main__':
    main()
