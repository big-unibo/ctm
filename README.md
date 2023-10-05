# Colossal Trajectory Mining

Colossal Trajectory Mining (CTM) detects co-movement patterns inside large scale trajectory datasets.
A spatio-temporal references is partitioned in tiles, each tiles has a fixed spatial area and a temporal size.
Then, for each tile is computed the set of trajectories whose at least one point is inside the tile boundaries.

Please refer/cite to the following research paper:

- Matteo Francia, Enrico Gallinucci. "Colossal Trajectory Mining: A Unifying Approach to Mine Behavioral Mobility Patterns." **Expert Systems with Applications** (to appear).

## Input dataset

The input dataset must be a table accessible on a Hive installation, 
the table must have the at least the following columns: 

* _userid: String_ contains a custom ID for every point of a trajectory
* _trajectoryid: String_ contains the trajectory ID for every point of a trajectory
* _latitude: Double_ contains the latitude of a point
* _longitude: Double_ contains the longitude of a point
* _timestamp: Long_ contains seconds elapsed since 1/1/1970 (unix_time) as a Long

## Output results

Output result are stored on a Hive table named: `CTM__par1_val1__...__parN_valN.csv`

## How to run this project

Unit tests are available via the `--debug` option on launch.

     rm results/CTM_stats.csv; git pull; ./gradlew

Experimental tests are available by running:
    
     rm results/CTM_stats.csv; git pull; ./gradlew clean build shadowJar; sh run_CTM.sh
