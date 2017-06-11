# Cassandra Schemas

## Users
1. username - text
2. created_time - timeuuid
3. full_name - text


PK - username

## User Outbound Follows
1. follower_username - text
2. followed_username - text


PK - follower\_username, followed\_username

## User Inbound Follows
1. followed_username - text
2. follower_username - text


PK - followed\_username, follower\_username

## User Status Updates
1. username - text
2. photo_id - timeuuid
3. photo_link - text
4. location - text
5. comments - list\<frozen\<map\<text, text>>>
6. likes - set\<text\>
7. tags - set\<text\>


PK - username, photo_id


Clustering Order By photo_id desc


## Home Status Updates
1. timeline_username - text
2. photo_id - timeuuid
3. photo_username - text
4. photo_link - text
5. photo_likes - set\<text\>
6. photo_comments - list\<frozen\<map\<text, text>>>
7. photo_location - text
8. photo_tags - set\<text\>


PK - timeline\_username, photo\_id, photo_username


Clustering Order By photo_id desc
