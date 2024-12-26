FROM python:3.13.1

ENV PATH="/root/.cargo/bin:${PATH}"

ENV PYTHONPATH=/app
RUN apt-get update && apt-get install -y cargo vim
# Get Rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
COPY ./requirements.txt /app/requirements.txt
COPY utils /app/utils
# RUN apt-get install -u build-essential curl

RUN pip install --upgrade pip \
   && pip install -r /app/requirements.txt



WORKDIR /app
CMD bash



