Foreign Data Wrapper for DB2 on Windows Operationg System
=========================================================

For the postgres on Windows i tested with MSYS2 and the MinGW-w64 compiler.

This README contains the following sections:

1. [MSYS2 Installation](#1-msys2)
2. [MinGW-w64 Installaiton](#2-Installation of MinGW)
3. [PostgreSQL from Source](#3-postgresql installation)

1 MSYS2 Installation
====================

From the downloadpage:
[MSYS2 HomePage](https://www.msys2.org/)
download the 64 Bit package. Install straight forward. Choose an installation path without a space.

More detailed information will be provided on the home page.

Open a MSYS2 64 Bit command window.
For updating the packages call:
pacman -Syu

to update the system call:
pacman -Su



2 MinGW-w64 Installaiton
========================
In a command window of MSYS2 enter the command:
pacman --needed -S git mingw-w64-x86_64-gcc base level

3 PostgreSQL from Source
========================
Download one of the source tar balls, uncompressit and deploy in one of the folder you created for build.

In the deployed folder execute the following command:
./configure --host=x86_64-w64-mingw32 --prefix= < installation directory>
like
./configure --host=x86_64-w64-mingw32 --prefix=/c/builds/Postgres/dist
Then execute
make clean all install
and
cd contrib
make clean all install

Copy the distribution or compress it in an zip File to copy it on another Server:
Set the environment variables in Windows PATH and generated an instanz with:
initdb --pgdata=c:\data\pgdata105 -username=postgres --auth=trust
