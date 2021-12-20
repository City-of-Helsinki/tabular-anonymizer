FROM ubuntu:20.04

WORKDIR /data
ENV VIRTUAL_ENV=/data/notebook/venv

# Install python & pip
RUN echo "Install python"
RUN apt update && apt install -y python3.8 python3-pip git
RUN python3 -m pip install --upgrade pip

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

EXPOSE 8888
CMD jupyter-lab --allow-root --ip 0.0.0.0