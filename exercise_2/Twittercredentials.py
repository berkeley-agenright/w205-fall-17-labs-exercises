import tweepy

consumer_key = "o8duUcpDRT0XD04MRLLp0cMtw";

consumer_secret = "Y57V0UG0BE5UrInfUC1t8bivgYpaWeDTpSYQ29ZUrESKTPxhd9";

access_token = "929129398174961664-vC5P2y1fTE22yWm4RjkYk1PB8ccrvO7";

access_token_secret = "RchaxbeHC5OsVZMRZcR0GxaPSL8GciLHjwLTr9UoBaDIH";

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)



