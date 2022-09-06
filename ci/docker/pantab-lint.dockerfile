FROM python
WORKDIR /pantab
RUN apt-get update
RUN apt-get install -y clang-format-11
RUN python -m pip install --upgrade pip
RUN python -m pip install \
  black==22.8.0 \
  mypy==0.940

