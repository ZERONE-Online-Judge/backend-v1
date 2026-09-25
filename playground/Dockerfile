FROM python:3.13-slim-bookworm
RUN pip install --no-cache-dir httpx==0.28.1
COPY service.py /opt/service.py
WORKDIR /opt
ENTRYPOINT ["python", "-I", "/opt/service.py"]
