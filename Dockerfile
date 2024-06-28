FROM python:3.11
WORKDIR /home
COPY . ./
RUN pip install .
ENTRYPOINT ["/bin/bash", "/home/start_services.sh"]
