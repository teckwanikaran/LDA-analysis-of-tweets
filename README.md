# LDA-analysis-of-tweets

- The main focus of this project is on one of the clustering model, known as Latent Dirichlet Allocation (LDA). 
- This project is carried out using the Databricks community edition notebook and all the code is written in Scala. 
- The cluster is created on the community edition which consist of one node with 6 Gb of processing power. 
- Here is the [link](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5511768504664716/800682239267153/1793597155194084/latest.html) to the Databricks notebook and the blog [link](https://karanteckwani.com/portfolio/lda-analysis-of-tweets-spark-streaming/) for this project. 

#### Structure of notebook:

- Use the spark streaming libraries with the Twitter developer API authenticator
- Stream live Twitter data from its platform
- Clean and isolate the English language tweets
- Batch stream the context and store in local directory as text files
- Separate the status/text of tweets into words to build the words LDA model
- Extract the hashtags from the tweets to build the hashtags LDA model
