FROM python:3.11-slim
WORKDIR /home
COPY . ./
RUN pip install .
RUN useradd -ms /bin/bash default
RUN mkdir logs
run chown default logs
USER default
ENTRYPOINT ["/bin/bash", "/home/start_services.sh"]
