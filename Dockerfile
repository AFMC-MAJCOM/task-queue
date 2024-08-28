FROM python:3.11-slim
RUN useradd -ms /bin/bash default
WORKDIR /home/default
COPY --chown=default:default . ./
USER default
RUN pip install .
ENTRYPOINT ["/bin/bash", "./start_services.sh"]
