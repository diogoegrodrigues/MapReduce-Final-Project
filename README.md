# MapReduce-Final-Project

## Necessary Modules
- module swap PrgEnv-cray PrgEnv-gnu
- module load cce/8.6.5
- module swap craype craype/2.5.14
- module swap cray-mpich cray-mpich/7.7.0

## Compiling the project
In the root folder execute the makefile with the command `make`

## Running the application
The execution file "map_reduce.out" is located in the bin folder.
The application has two flags:
- -i - which is the input file
- -r - the number of repetitions

The output file is also located in the bin folder.
