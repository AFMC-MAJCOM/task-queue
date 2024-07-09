FROM python:3.11
WORKDIR /home
COPY . ./
RUN pip install .
<<<<<<< HEAD
=======
RUN useradd -ms /bin/bash default
USER default
>>>>>>> 269fb13d442e269e7e9f06787dedca59638d307a
ENTRYPOINT ["/bin/bash", "/home/start_services.sh"]
