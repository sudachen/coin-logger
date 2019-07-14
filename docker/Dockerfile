FROM ubuntu:18.04
LABEL maintainer="Alexey Sudachen <alexey@sudachen.name>"
ENV TZ=America/Santiago
ENV SHELL=/bin/bash
ENV HOME=/home/u

RUN bash -c "for i in {1..9}; do mkdir -p /usr/share/man/man\$i; done" \
 && apt-get update --fix-missing \
 && apt-get install -qy --no-install-recommends \
    ca-certificates \
    tzdata \
    locales \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* \
 && locale-gen en_US.UTF-8 \
 && update-locale LANG=en_US.UTF-8 \
 && ln -snf /usr/share/zoneinfo/${TZ} /etc/localtime && echo ${TZ} > /etc/timezone \
 && useradd -m -s ${SHELL} -N -u 1000 u -d ${HOME}

ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ENV USER=u

USER $USER
WORKDIR $HOME
COPY --chown=1000:100 ./coin-logger ./
VOLUME ["/etc/infra", "/data"]
CMD ["/home/u/coin-logger", "do-coin-logger"]

