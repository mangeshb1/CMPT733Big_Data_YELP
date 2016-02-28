Final Project: Initial Plan

Ratatouille: What’s Actually Cooking in the Kitchen
We look to create a classification model that can predict when a restaurant should be inspected by health officials based on combining historical inspection schedules with Yelp reviews and ratings for restaurants in the cities of Boston (MA) and Las Vegas (NV).

Mangesh Bhangare, Saif Charaniya, Jay Pratap Naidu

Cities are responsible for distributing restaurant licenses and performing regular inspections to ensure a safe environment for customers while maintaining a high standard of food processing to reduce the chances of food borne illnesses.  A major hurdle in this is determining how frequent and how regular to conduct inspections on particular restaurants.  Public health offices often uses customer feedback to determine the appropriate time for inspection on a case by case situation, usually done when a customer has experienced a violation first hand (like in food poisioning).  However, public health can now harness the power of social media to determine when a restaurant may need an inspection based on how the restaurant is reviewed and rated.  Many sites such as Yelp, Yellow Pages, and Google, offer their users the ability to express their happiness or concerns and disappointments.  Our major question in this project thus revolves around how we can create a model to determine when a restaurant would require an updated health inspection, based particularly on a restuarants historical inspection schedule and violations and on an analysis of how the restuarant reviews and ratings evolve over time.  Motivation arises from the recent competition hosted by the City of Boston and Yelp on DrivenData.org and from the recent uprsing in major food chains having health related issues, such as those in the case of Chipolte.
Which restaurants have the best track record in terms of service and reputation? Which restaurants need lesser inspections thus saving time and finding the restaurants that do need health inspections? The model developed would recommend where to inspect along with the frequency of inspections required. This would help finding restaurants which missed checks because of the current random checks system that is followed and evolve into a better system covering more targets and increasing the scope.
How do factors such as tips given, user scores, and review text allow us to rank restaurants and provide customers with valuable feedback. A combination of them would be inherent to a sound model being generated. Overall, this would greatly help any health department in their day to day operations and improve the health inspection system by covering the right spots to check.

Data is collected from YELP academic dataset, from City of Boston and Las Vegas 
Data Cleaning: Spark, Open Refine 
Data Storage: apache parquet or HBASE
Data Visualization: R shiny Server or Web application, with a portal allowing users to find out how their choice of restaurant compares in terms of its health status.

Data Collected: 
The YELP dataset, City of Boston Restaurant Violations and Inspections, and City of Las Vegas Restaurant Violations and Inspections have been obtained so far. 

Data Cleaning: 
Removing incomplete data, combining Yelp reviews with the corresponding restuarant they refer to in the city data of violations and inspections, and performing geotagging on restaurant locations in Las Vegas.

Data Integration: 
We look to create a systematic way in providing labels to our dataset from Las Vegas. These labels with correspond to the performance of the restaurant and describe whether its risk factor for inspection (example high risk, low risk, etc).  To determine the labels we will create a schema that can be used to effectively classify a restaurant based on its inspection history.  This will most likely be a manual process in which we can take advantage of crowdsourcing.   

Data Analysis : 
Multiclass Classification:
We look to create a classification method for determining whether a the risk level of a restaurant.  To do this we run run a parameter selection routine on our Las Vegas Dataset.  We will split the Las Vegas dataset into a training and testing set which we help use quantify the results of our classification model.  Our classification model with determine which classification process (ie Random Forests, Naive Bayes) is optimal for the task, and how we can gain performance with the use of boosting (such as AdaBoost).  
Once our classification routine is optimized, we will be able to use the model to determine which restaurants in the city of Boston belong to which classes based on their parameters.  This will give us a prediction of which restaurants we believe should be inspected.
Geo Clustering:
We will use geotagging to perform geo clustering of restaurants based on their class label.  This will allow us to see how restaurants with similar classes are distributed around the city.
Clustering based on Yelp Reviews:
We will perform clustering based on our parameters from classification to determine how groups of restaurants with similar Yelp reviews are connected with each other.  This is an interesting way to see how restaurants which may not be in the same class of violations are clustered together based on their parameters from the classification model above, and can also be used to find anomalies such as restaurants that receive too much in tips compared to their user satisfaction.

Data Product :  
We intend to create the user interface in R Shiny server or Web application in PHP where the user can search for a particular restaurant and it will give average YELP score as well as Risk rating associated with each restaurant which can be used by the user as well as city health inspectors to decide should they visit a particular restaurant or not. It will also include a summary of other measures calculated such as restaurant  based on geo-clustering and some interesting anomalies that we found out with our analysis. 

 
