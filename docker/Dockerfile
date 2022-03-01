FROM ubuntu:latest as cached
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update
RUN apt-get install -y git
RUN apt-get install -y python3
RUN apt-get install -y pip
RUN git clone https://github.com/BeeGroup-cimne/beeway_cloud
WORKDIR beeway_cloud
RUN pip install -r requirements.txt
FROM cached
ADD "https://www.random.org/cgi-bin/randbyte?nbytes=10&format=h" skipcache
RUN git pull