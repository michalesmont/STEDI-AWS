Project overview:

This is a part of Udacity Training.
In particular project, the goal is to build a data lakehouse solution for sensor data that trains a machine learning model.

The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

    - trains the user to do a STEDI balance exercise;
    - and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
    - has a companion mobile app that collects customer data and interacts with the device sensors.

STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

Resources in the repository:

1. /starter/ - a folder containing data to be uploaded in the S3 bucket cloud solution.
2. /sql/ - queries for Glue tables creation in AWS Athena.
3. /py_scripts/ - scripts for AWS Glue making data operations.
4. /screens/ - summaries of Glue data tables created in AWS Athena.
