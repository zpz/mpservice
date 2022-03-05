FROM zppz/py3:22.02.27
USER root

ARG PROJ

COPY . /src/

RUN cd /src \
    && python -m pip install -e . \
    && python -m pip uninstall -y ${PROJ}  \
    && python -m pip install -r /src/requirements-test.txt


USER docker-user
