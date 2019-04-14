# COMET
Algorithm for matching contextually similar RDF Molecules

## Dependencies

 1. [GADES](https://github.com/RDF-Molecules/sim_service) similarity metric service
 2.  [concepts](https://github.com/xflr6/concepts) by xflr6
 3. JDK 1.8
 4. Python 2.7
 5. Activator 1.3.12

## Configuration

### Setup GADES

 1. Download [the GADES service project](https://github.com/RDF-Molecules/sim_service).
 2. Download and unzip [Activator 1.3.12](https://downloads.typesafe.com/typesafe-activator/1.3.12/typesafe-activator-1.3.12.zip) into the `sim_service` project folder.
 3. Add the following configuration to `application.conf`

    >      model1_location = "~/COMET/context_evaluation/1000/2-2/dataset1_1000.nt"  
    >      model2_location = "~/COMET/context_evaluation/1000/2-2/dataset2_1000.nt"
	Replace ~ with your local path to COMET's root folder.

 4. From the sim_service project folder, run:
		 
		 $ activator run
	Make sure `port 9000` is free.
### Install concepts
Run the following command to install the concepts package:
		
		$ pip install concepts


## Run COMET
From any command line window, run:

	$ python testcomet.py
	
