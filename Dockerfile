#!/usr/bin/env -S bash -c "docker rm db2_fdw_run; docker build -t db2_fdw . && docker run --name db2_fdw_run db2_fdw && docker cp db2_fdw_run:/workspace/db2_fdw.so ."

FROM ibmcom/db2
ENV LICENSE accept
ENV DB2INST1_PASSWORD first
ENV DBNAME dbname

# Install the repository RPM:
RUN dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-8-x86_64/pgdg-redhat-repo-latest.noarch.rpm \
# Disable the built-in PostgreSQL module:
&& dnf -qy module disable postgresql \
&& dnf install epel-release -y\
&& dnf install -y gcc-toolset-11-gcc curl patch make perl git python2 m4 cpp kernel-devel kernel-headers gcc-c++ \
&& useradd -m -s /bin/bash linuxbrew && echo 'linuxbrew ALL=(ALL) NOPASSWD:ALL' >>/etc/sudoers

# Install linuxbrew:

USER linuxbrew
ENV PATH="/home/linuxbrew/.linuxbrew/bin:/home/linuxbrew/.linuxbrew/sbin:/home/linuxbrew/.linuxbrew/opt/postgresql@15/bin:$PATH"
RUN sh -c "$(curl -fsSL https://raw.githubusercontent.com/Linuxbrew/install/master/install.sh)" \
&& brew install curl postgresql@15 gcc@11 && brew link binutils --force

USER root

COPY . /workspace
WORKDIR /workspace
ENV DB2_HOME=/opt/ibm/db2/V11.5
# setup for db2 as well as paths for compiles
#RUN eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)" && chmod -R a+w /workspace && make -C /workspace
ENTRYPOINT ["make", "-C", "/workspace"]
# This the Entrypoint from ibmcom/db2; Generate as a , script but didn't need this to start a service
#RUN echo /var/db2_setup/lib/setup_db2_instance.sh > $HOME/, && chmod a+x $HOME/,


