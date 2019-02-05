from __future__ import print_function
import os
import json
import shlex
import socket
import subprocess
from common_utils.s3_utils import download_file,get_aws_session,file_exists

def fixResolv():
    """
    Fix issue with gethostname() in MPI calls
    """
    with open("/etc/hosts", "a") as hostsfile:
       hostsfile.write('127.0.0.1 %s\n' % (socket.gethostname()) )

def exportSession():
    if not os.getenv('AWS_SESSION_TOKEN'):
        cred = get_aws_session()
        os.environ['AWS_ACCESS_KEY_ID'] = cred.access_key
        os.environ['AWS_SECRET_ACCESS_KEY'] = cred.secret_key
        os.environ['AWS_SESSION_TOKEN'] = cred.token

        fixResolv()

def run_vcf2tiledb_basic(idx, loader_path):
    """
    Runs vcf2tiledb
    :param loader_path: s3 path to loader.json
    :param idx: index in loader
    """
    print ("Running vcf2tiledb")
    exportSession()

    cmd = 'vcf2tiledb -r%d %s' % (idx, loader_path)

    print('Running cmd:= ', end='')
    print(cmd)

    try:
        subprocess.check_call(shlex.split(cmd))
        return True
    except subprocess.CalledProcessError, e:
        if e.returncode in [-11, -6, 1]:
            # Possible connection issue
            print("ReturnCode: %s" % e.returncode)
            return False
        elif e.returncode == -9:
            print("Error. Possible memory limit issue.")
            print("ReturnCode: -9")
            raise
        else:
            raise


def run_vcf2tiledb_no_s3(workdir,idx, loader_path, callset_path, vid_path, contig):
    """
    fallback to downloading each file. This method uses
    tabix to read the intervals from each file and saves sliced vcf
    as a local file.
    """
    print("Performing no-s3-callset vcf2tiledb")
    exportSession()

    with open(vid_path) as vid_file:
        hg = json.load(vid_file)

    offset = hg['contigs'][contig]['tiledb_column_offset']

    # edit loader file to point to correct callset filename
    srchStr = subprocess.check_output('grep -Po "/callset.+.json" %s' % (loader_path), shell=True)
    subprocess.check_call('sed -i "s|%s|%s|" %s' % (srchStr.rstrip(), callset_path, loader_path), shell=True)

    # extract start/end for this partition
    with open(loader_path) as loader_file:
        ldr = json.load(loader_file)

    start = ldr['column_partitions'][idx]['begin'] - offset - 1
    end   = ldr['column_partitions'][idx]['end']   - offset + 1
    del ldr

    if start < 0: start = 0

    pos = "%s:%s-%s" % (contig, start, end) # tabix region

    # Download VCF slices to workdir
    # assumes callset has S3 paths for filenames
    with open(callset_path) as callset_fp:
        fList = json.load(callset_fp)
        fListNew = fList

    print("Downloading slices")
    for SM in fList['callsets']:
        #download_file(fList['callsets'][SM]['filename'], workdir)
        s3path = fList['callsets'][SM]['filename']
        fName = os.path.basename(s3path)
        gzFile = '%s/%s' % (workdir, fName)

        retries = 0
        while not os.path.exists(gzFile) or os.stat(gzFile).st_size < 29:
          if retries > 0: print("Retrying download for {}".format(fName))

          cmd = 'tabix -h %s %s | bgzip > %s' % (s3path, pos, gzFile)
          subprocess.check_call(cmd, shell=True)
          retries += 1
          if retries > 3:
              print('Downloading entire file')
              download_file(s3path, workdir)
              break


        # remove side-downloaded tbi
        subprocess.check_call('rm -f /%s.tbi' % (fName) ,shell=True)

        # Build index for this new file
        cmd = 'tabix -f -C %s/%s' % (workdir, fName)
        subprocess.check_call(shlex.split(cmd))

        #record new callset location
        fListNew['callsets'][SM]['filename'] = workdir + '/' + fName

    # Re-write callsets to point to local file
    print("Updating downloaded callset file")
    with open(callset_path, 'w') as callset_fp:
        json.dump(obj=dict(
                        fListNew
                      ),
                  indent=2,
                  sort_keys=True,
                  separators=(',', ': '), fp=callset_fp)

    # Run cmd
    cmd = 'vcf2tiledb -r%d %s' % (idx, loader_path)

    print('Running cmd:= ', end='')
    print(cmd)
    subprocess.check_call(shlex.split(cmd))

    print("Cleaning up interval files")

    cmd = 'find %s/ -name "*.g.vcf.gz*" -delete' % (workdir)
    subprocess.check_call(cmd, shell=True)

    return True

def checkUploadExists(loader_path, idx, results_path):
    with open(loader_path) as loader_file:
        ldr = json.load(loader_file)

    localFileName = ldr['column_partitions'][idx]['vcf_output_filename']
    uploadFileName = os.path.basename(localFileName)
    return file_exists(results_path.rstrip('/') + '/' + uploadFileName.lstrip('/'))

