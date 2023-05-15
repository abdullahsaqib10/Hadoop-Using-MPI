#include <sys/time.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main( int argc, char **argv )
{
    srand(time(0));

    int arraySize = 16; //8*8 array

    //creating the two arrays
    int **array1 = (int **) malloc(arraySize * sizeof(int *));
    for (int i =0; i < arraySize; i++)
    {
        array1[i] = (int *) malloc(arraySize * sizeof(int));
    }

    int **array2 = (int **) malloc(arraySize * sizeof(int *));
    for (int i =0; i < arraySize; i++)
    {
        array2[i] = (int *) malloc(arraySize * sizeof(int));
    }


    //initializing the array with random numbers
    for (int i =0; i < arraySize; i++)
    {
        for (int j =0; j < arraySize; j++)
        {
            array1[i][j] = (rand() % (200)) + 10;
            array2[i][j] = (rand() % (200)) + 10;
        }
    }


    //Writing to file
    FILE *file;

    file = fopen("matrix1.txt","w");
    fprintf(file,"%d,%d\n",arraySize, arraySize);
    for (int i =0; i < arraySize; i++)
    {
        for (int j =0; j < arraySize; j++)
        {
            if (j != arraySize - 1)
		    	fprintf(file,"%d,",array1[i][j]);
		    else
		    	fprintf(file,"%d",array1[i][j]);
        }
        fprintf(file,"\n");
    }

    file = fopen("matrix2.txt","w");
    fprintf(file,"%d,%d\n",arraySize, arraySize);
    for (int i =0; i < arraySize; i++)
    {
        for (int j =0; j < arraySize; j++)
        {
            if (j != arraySize - 1)
		    	fprintf(file,"%d,",array2[i][j]);
		    else
		    	fprintf(file,"%d",array2[i][j]);
        }
        fprintf(file,"\n");
    }



    return 0;
}


