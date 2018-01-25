#!/bin/bash
for a in $*
do
sedfile=$a.sed
sed -f ./utils/ch2.sed $a > $sedfile
mv $sedfile $a
done

