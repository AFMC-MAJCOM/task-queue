FROM python:3.11
WORKDIR /home
COPY . ./
RUN pip install .
RUN useradd -ms /bin/bash default
USER default
ENTRYPOINT ["/bin/bash", "/home/start_services.sh"]
