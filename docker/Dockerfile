FROM python:latest
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get install git
RUN git clone https://github.com/BeeGroup-cimne/beeway_cloud
WORKDIR beeway_cloud
RUN pip install -r requirements.txt