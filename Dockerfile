FROM ottov/genomicsdb-s3:latest
MAINTAINER Otto Valladares <ottov.upenn@gmail.com>
LABEL Description="GenomicsDB with S3, built for AWS ECS, based on Ubuntu 16.04 LTS." \
        License="Apache License 2.0" \
        Version="1.0"

# update OS
ENV TERM dumb
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y python-pip libgomp1 && \
    apt-get remove -y --purge gcc-5 g++-5 libpython2.7-dev libicu55 libpython2.7-dev libssl-dev libc6-dev && \
    apt-get -y autoremove && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --disable-pip-version-check boto3 awscli

COPY ./run_vcf2tiledb.py /
COPY ./common_utils /common_utils
COPY tabix bgzip /usr/local/bin/

ENTRYPOINT ["python","-u", "run_vcf2tiledb.py"]
