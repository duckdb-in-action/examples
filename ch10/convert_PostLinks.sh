#!/bin/bash

7z e -so stackoverflow.com-PostLinks.7z | \
xidel -se '//row/[(@Id|@CreationDate|@PostId|@RelatedPostId|@LinkTypeId)]' - | \
(echo "Id,CreationDate,PostId,RelatedPostId,LinkTypeId" &&
jq -r '. | @csv') |
gzip -9 > PostLinks.csv.gz
