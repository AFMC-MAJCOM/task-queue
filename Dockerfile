FROM python:3.11-slim
RUN useradd -ms /bin/bash default
WORKDIR /home/default
COPY --chown=default:default . ./
RUN add-apt-repository ppa:git-core/ppa; apt-get update; apt-get install -y git 
USER default
RUN mkdir /home/default/logs; \
    python -m pip install -U pip; \
    pip install .; \
    rm -rf .git
ENTRYPOINT ["/bin/bash", "./start_services.sh"]
