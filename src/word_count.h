#ifndef __WORD_COUNT_H__
#define __WORD_COUNT_H__

#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>

#include <mpi.h>

char** read_file();
void tokenize();

#endif