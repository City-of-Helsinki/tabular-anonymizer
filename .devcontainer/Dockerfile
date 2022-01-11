FROM ubuntu:20.04

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID




RUN apt-get update --fix-missing && \ 
apt-get -y upgrade && \
apt-get install -y --no-install-recommends build-essential software-properties-common python-is-python3 python3-pip python3-dev git && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

# Create the user
RUN groupadd --gid $USER_GID $USERNAME && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME
 
USER $USERNAME
