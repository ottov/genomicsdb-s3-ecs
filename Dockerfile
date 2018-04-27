FROM ottov/genomicsdb-s3:latest
MAINTAINER Otto Valladares <ottov.upenn@gmail.com>
LABEL Description="GenomicsDB with S3, built for AWS ECS, based on Ubuntu 16.04 LTS." \
        License="Apache License 2.0" \
        Version="1.0"

# update OS
ENV TERM dumb
RUN apt update
RUN apt upgrade -y
RUN apt install -y python-pip

RUN rm -rf /var/lib/apt/lists/*

RUN pip install boto3 awscli

COPY run.py /run.py
COPY common_utils /common_utils

ENTRYPOINT ["python","-u", "run.py"]
