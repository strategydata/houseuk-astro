FROM astrocrpublic.azurecr.io/runtime:3.1-8
USER root
RUN apt-get update && \
    apt-get install -y curl telnet netcat-openbsd && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
USER astro