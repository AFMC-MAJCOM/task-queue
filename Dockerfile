FROM python:3.11-slim
RUN useradd -ms /bin/bash default
WORKDIR /home/default
COPY --chown=default:default . ./
USER default
RUN mkdir /home/default/logs
RUN pip install .[sql,s3]
ENTRYPOINT ["/bin/bash", "./start_services.sh"]