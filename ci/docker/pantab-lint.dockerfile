FROM python
WORKDIR /pantab
RUN python -m pip install --upgrade pip
RUN python -m pip install \
  black==22.1.0 \
  isort==5.10.1 \
  mypy==0.940

