# spark4achilles

## Objective
Runs variations of ACHILLES analytics using Scala + Spark

## How to run
First, sbt will bring in everything you need. Then you need to build the JAR:
```bash
sbt assembly
```

Then to run with Spark:
```bash
spark-submit --class edu.gatech.cse8803.main.Main cse8803_project-assembly-1.3.jar
```

## Contributors
* [Joshua Powers](http://powersj.github.io/)
  * CSE8803 Big Data Analytics for Health Care  (Spring 2016)
  * Georgia Institute of Technology

## Reviewers
A huge thank you to the following for their feedback, evaluation, and support:
 * Dr. Jimeng Sun
 * The SunLab
 * Dr. Watler & Marjie Powers
 * Olga Martyusheva
 * Alex Balderson

# License
Apache 2.0 &copy; 2016 Joshua Powers