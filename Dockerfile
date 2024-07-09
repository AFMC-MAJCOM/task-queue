FROM python:3.11-slim
WORKDIR /home
COPY . ./
RUN pip install .
RUN useradd -ms /bin/bash default
USER default
ENTRYPOINT ["/bin/bash", "/home/start_services.sh"]
HEALTHCHECK CMD curl --fail http://localhost:8001 || exit 1
