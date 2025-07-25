#!/bin/bash

7z e -so stackoverflow.com-Badges.7z | \
xidel -se '//row/[(@Id|@UserId|@Name|@Date|@Class|@TagBased)]' - | \
(echo "Id,UserId,Name,Date,Class,TagBased" &&
jq -r '. | @csv') |
gzip -9 > Badges.csv.gz
