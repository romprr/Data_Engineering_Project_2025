# DataEng 2024 Template Repository

![Insalogo](./images/logo-insa_0.png)

Project [DATA Engineering](https://www.riccardotommasini.com/courses/dataeng-insa-ot/) is provided by [INSA Lyon](https://www.insa-lyon.fr/).

Students: 
- Adrian Abi Saleh
- Romain Poirrier
- Aymerick Yzidee

### Abstract

[TODO]

## Datasets Description

### UCDP datasets

The datasets choosen for the conflicts are the UCDP datasets, especially these ones : 
- Yearly Armed Conflicts
- Georeferenced Events
- Actors

These datasets are complementary and are used to enrich the data, they contained armed conflicts by year, informations about every actors in the dataset, and finally specific information about every (or almost every) battle that can be found in the datasets available on UCDP.

### Yahoo Finance

[TODO]

## Queries

We choose to make 3 queries : 
[TODO]

## Requirements

## Note for Students

- Clone the created repository offline;
- Add your name and surname into the Readme file and your teammates as collaborators
- Complete the field above after project is approved
- Make any changes to your repository according to the specific assignment;
- Ensure code reproducibility and instructions on how to replicate the results;
- Add an open-source license, e.g., Apache 2.0;
- README is automatically converted into pdf

## CONNECTION TO INSIGHTS

### Postgres

- Put the credentials
- Create a new connection using the database information
- Query the stock database

### Mongo

- Open the container's command line
- Run the following command with the right credentials (mongosh "mongodb://USERNAME:PASSWORD@localhost:PORT/DB_NAME?authSource=DATABASE_NAME_FOR_AUTH")

### Redis

- Opent the container's command line
- Run the follow command with the right credentials (redis-cli --user username --pass password)
- RPUSH and RPOP on different queues to manage the data
