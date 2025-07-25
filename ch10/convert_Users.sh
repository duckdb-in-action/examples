#!/bin/bash

7z e -so stackoverflow.com-Users.7z | \
xidel -se '//row/[(@Id|@Reputation|@CreationDate|@DisplayName|@LastAccessDate|@AboutMe|@Views|@UpVotes|@DownVotes|@WebsiteUrl|@Location|@AccountId)]' - | \
(echo "Id,Reputation,CreationDate,DisplayName,LastAccessDate,AboutMe,Views,UpVotes,DownVotes,WebsiteUrl,Location,AccountId" &&
jq -r '. | @csv') |
gzip -9 > Users.csv.gz
