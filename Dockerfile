FROM python:3.11-slim
RUN useradd -ms /bin/bash default
USER default
WORKDIR /home/default
COPY --chown=default:default . ./
RUN pip install .
ENTRYPOINT ["/bin/bash", "./start_services.sh"]