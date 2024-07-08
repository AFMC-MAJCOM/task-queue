FROM python:3.11
WORKDIR /home
COPY . ./
RUN useradd -ms /bin/bash default
USER default
RUN pip install .
ENTRYPOINT ["/bin/bash", "/home/start_services.sh"]
