# README

## Data Generation
1. Users
   - ID
   - Name
   - Full Name
   - Created Time
   - Followers
     - Follower ID
     - Name
     - Full Name
     - Created Time

2. Photo Event
   - User
       - ID
       - Name
       - Full Name
   - Tags
   - ID
   - Link
   - Created Time
   - Location

3. Comment Event
   - Created Time
   - Text
   - ID
   - User
       - ID
       - Name
       - Full Name
   - Photo
       - ID
       - Tags
       - Link
       - Created Time
       - Location
       - User
           - ID
           - Name
           - Full Name

4. Like Event
   - User
       - ID
       - Name
       - Full Name
   - Created Time
   - ID
   - Photo
     - ID
     - Tags
     - Link
     - Created Time
     - Location
     - User
         - ID
         - Name
         - Full Name

5. Follow Event
   - ID
   - Created Time
   - User
       - ID
       - Name
       - Full Name
   - Followee
       - ID
       - Name
       - Full Name

6. Unfollow Event
   - ID
   - Created Time
   - User
       - ID
       - Name
       - Full Name
   - Followee
       - ID
       - Name
       - Full Name