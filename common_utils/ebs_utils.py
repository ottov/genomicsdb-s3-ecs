import os
import json
import time
from common_utils.s3_utils import get_size

def initEBS(WORKDIR):
    if os.getenv('EBSSIZE'):
        total_size = int(os.getenv('EBSSIZE')) * 1024**3 # EBS SIZE is in GB
    else:
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
