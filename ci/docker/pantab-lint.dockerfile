FROM python
WORKDIR /pantab
RUN apt-get update
RUN apt-get install -y clang-format-11
RUN python -m pip install --upgrade pip
RUN python -m pip install \
  black==22.1.0 \
  isort==5.10.1 \
  mypy==0.940

