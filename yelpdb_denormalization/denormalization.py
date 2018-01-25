#!/usr/bin/env python3




"""
    lab7.py - Python3 program
    Author: Aayush Baral (aayushbaral@bennington.edu)
    Created: 11/22/2017
"""

from pymongo import MongoClient
from pymongo import InsertOne, DeleteOne, UpdateOne

#Initializing a Mongo client
client = MongoClient()
db = client.yelp_database
businesses = db.business
review = db.review
user = db.user



def addReviews(business, reviews):
	try:

		"""Tried updating and deleting reviews at the same time. Won't work with testing though"""

		
		business_details = business.find()
		#A dictionary to store the business ids and review counts from review table
		count_review_table = {}

		print("Starting the denormalization process...")
			
		for i in business_details:
			review_details = review.find({"business_id": i["business_id"]})
			review_details_count = review_details.count()
			#Storing the review counts
			count_review_table[i["business_id"]] = review_details_count

			"""I used an array to store all the reviews of a single business"""
			reviews_list = []
			for j in review_details:
				#Adding user name for each reviews
				user_id = j["user_id"]
				user_name = user.find_one({"user_id": user_id})["name"]
				j["user_name"] = user_name
				if j not in reviews_list:
					reviews_list.append(j)
			
			"""Simulataneously updates and deletes each of the business with business id i	
			update_one only updates one business at a time. delete_many deletes multiple reviews form a
			single business in this case
			"""	
			review.delete_many({"business_id": i["business_id"]})	
			businesses.update_one({"business_id": i["business_id"]}, {'$set': {"Reviews": reviews_list}})

		print("Denormalization process completed")


	# """For testing Instead of a test function I added this code. I think this is not an efficient way to do this 
	# 	but I had a hard time getting the review count in a different function by calling the addReviews
	# 	function as the reviews are already deleted
	# """

		print("Testing has started...")
		review_count = {}

		
		updated_business_details = businesses.find()
		for j in updated_business_details:
			review_count[j["business_id"]] = len(j["Reviews"])
		for key in review_count:
			if key in count_review_table:
				if(review_count[key] == count_review_table[key]):
					print("All reviews have been added")
					return True

	except Exception as e:
		print(e)

if __name__ == '__main__':
	result = addReviews(businesses, review)
	print(result)
	



