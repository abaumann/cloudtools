FROM google/cloud-sdk:170.0.1-slim

RUN pip install --upgrade google-api-python-client
ADD . /cloudtools/
RUN cd /cloudtools; python setup.py install

# Tell gcloud to save state in /.config so it's easy to override as a mounted volume.
ENV HOME=/