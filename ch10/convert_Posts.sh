#!/bin/bash

7z e -so stackoverflow.com-Posts.7z | \
xidel -se '//row/[(@Id|@PostTypeId|@AcceptedAnswerId|@ParentId|@CreationDate|@Score|@ViewCount|@Body|@OwnerUserId|@LastEditorUserId|@LastEditorDisplayName|@LastEditDate|@LastActivityDate|@Title|@Tags|@AnswerCount|@CommentCount|@FavoriteCount|@CommunityOwnedDate|@ContentLicense)]' - | \
(echo "Id,PostTypeId,AcceptedAnswerId,ParentId,CreationDate,Score,ViewCount,Body,OwnerUserId,LastEditorUserId,LastEditorDisplayName,LastEditDate,LastActivityDate,Title,Tags,AnswerCount,CommentCount,FavoriteCount,CommunityOwnedDate,ContentLicense" &&jq -r '. | @csv') |
gzip -9 > Posts.csv.gz
