FROM mcr.microsoft.com/vscode/devcontainers/python:3.9

WORKDIR /data
ENV VIRTUAL_ENV=/data/notebook/venv

# Setup project directory & venv
RUN echo "Setup project venv"
RUN mkdir -p /data/notebook

# Install project dependencies
RUN echo "Install jupyterlab"
RUN pip3 install jupyterlab


COPY ./tabular_anonymizer/ /data/tabular_anonymizer/tabular_anonymizer/
COPY ./setup.py /data/tabular_anonymizer/
COPY examples/sample_notebook.ipynb /data/notebook/
COPY examples/adult.csv /data/notebook/


RUN pip3 install jupyterlab
RUN pip3 install -e /data/tabular_anonymizer/
