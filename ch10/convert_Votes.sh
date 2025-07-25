#!/bin/bash

7z e -so stackoverflow.com-Votes.7z | \
xidel -se '//row/[(@Id|@PostId|@VoteTypeId|@CreationDate|@UserId|@BountyAmount)]' - | \
(echo "Id,PostId,VoteTypeId,CreationDate,UserId,BountyAmount" &&
jq -r '. | @csv') |
gzip -9 > Votes.csv.gz
