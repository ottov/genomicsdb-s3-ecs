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
RUN apt remove -y --purge g++-5
RUN apt -y autoremove

RUN rm -rf /var/lib/apt/lists/*

RUN pip install boto3 awscli

COPY ./run_vcf2tiledb.py /run_vcf2tiledb.py
COPY common_utils /common_utils
COPY tabix /usr/local/bin/
COPY bgzip /usr/local/bin/

ENTRYPOINT ["python","-u", "run_vcf2tiledb.py"]
