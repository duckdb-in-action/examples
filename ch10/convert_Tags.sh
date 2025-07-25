#!/bin/bash

7z e -so stackoverflow.com-Tags.7z | \
xidel -se '//row/[(@Id|@TagName|@Count|@ExcerptPostId|@WikiPostId)]' - | \
(echo "Id,TagName,Count,ExcerptPostId,WikiPostId" &&
jq -r '. | @csv') |
gzip -9 > Tags.csv.gz
