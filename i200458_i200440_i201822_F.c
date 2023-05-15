#include "mpich/mpi.h"
#include <sys/time.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sched.h> 
#include<pthread.h>
#include <time.h>
#include<math.h>
#include <errno.h>
#include <assert.h>
#include <string.h>
#include <stdbool.h>
#include <stdio.h>
#include <math.h>






void readFile(FILE * file, int **array)
{
    char line[999999] = {0};
		char num[4] = {0};
		int lineCount = 0;
        

        while(fgets(line, 999999, file))
		{
			int count = 0;
			int count2 = 0;
            int arrayCount = 0;
            //if (lineCount != 0)
            {
                while(line[count] != '\n')
				{
					//printf("\nthis: %c", line[count+1]);
					if (line[count] != ',')
					{
						num[count2] = line[count];
						count2++; 
					}
					else if (line[count+1] == '\n' || line[count] == ',')
					{
						count2 = 0;
						array[lineCount][arrayCount] = atoi(num);
						arrayCount++;
					}
					
				
					count++;
				}
				array[lineCount][arrayCount] = atoi(num);
				arrayCount++;

            }
            lineCount++;
        }
}

bool isMapper(int rank, int *mapperIds, int mapperNum)
{
    for (int i = 0; i< mapperNum; i++)
    {
        if (rank == mapperIds[i])
            return true;
    }
    return false;
}


bool isReducer(int rank, int *reducerIds, int reducerNum)
{
    for (int i = 0; i< reducerNum; i++)
    {
        if (rank == reducerIds[i])
            return true;
    }
    return false;
}

bool matrixComparison(int *a, int *b, int row)
{
    for (int i=0; i < row; i++)
    {
        for (int j = 0; j < row; j++)
        {
            if (a[i*row+j] != b[i*row+j])
                return false;
        }
    }
    return true;
}


int main( int argc, char **argv )
{
    srand(time(0));

    // Initialize the MPI environment

	MPI_Init(NULL, NULL);

	// Get the number of processes
	int worldSize;
	MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

	// Get the rank of the process
	int currRank;
	MPI_Comm_rank(MPI_COMM_WORLD, &currRank);

    int len;
    char machineName[MPI_MAX_PROCESSOR_NAME];

    int mapperNum=0;
    int reducerNum=0;
    int mode;

    int **array1, **array2;
    FILE *file;
    int row, col;


    //Command line input
	
	char* file1 = "";
    char* file2 = "";
	if (argc == 3)
	{
		file1 = argv[1];
		file2 = argv[2];
		//leafSize = pow(2, rollNumber+1);
		//printf("procces %d each with leaf size: %d\n", currRank, leafSize);
	}
	else
	{
		return 0;
	}


    //reading the input files and deciding the number of mappers and reducers
    if (currRank == 0)
    {
        MPI_Get_processor_name(machineName, &len);
        printf("Master with processId %d running on %s\n", currRank, machineName);

        int arraySize;

        //Reading array from file
        file = fopen(file1, "r");
	
		fscanf(file, "%d,%d\n", &row, &col);
        arraySize = row;

        array1 = (int **) malloc(row * sizeof(int *));
        for (int i =0; i < row; i++)
        {
            array1[i] = (int *) malloc(col * sizeof(int));
        }
        
        readFile(file, array1);

        file = fopen(file2, "r");

        fscanf(file, "%d,%d\n", &row, &col);

        array2 = (int **) malloc(row * sizeof(int *));
        for (int i =0; i < row; i++)
        {
            array2[i] = (int *) malloc(col * sizeof(int));
        }
        
        readFile(file, array2);

        fclose(file);

        // printf("\nFIRST ARRAY:\n");
        // for (int i =0; i< row; i++)
        // {
        //     for (int j = 0; j < col; j++)
        //     {
        //         if (j != col - 1)
        //             printf("%d," ,array1[i][j]);
        //         else
        //             printf("%d" ,array1[i][j]);
        //     }
            
        //     printf("\n");
        // }

        // printf("\nSECOND ARRAY:\n");
        // for (int i =0; i< row; i++)
        // {
        //     for (int j = 0; j < col; j++)
        //     {
        //         if (j != col - 1)
        //             printf("%d," ,array2[i][j]);
        //         else
        //             printf("%d" ,array2[i][j]);
        //     }
            
        //     printf("\n");
        // }


        //Deciding the number of mappers and reducers
        // printf("1.Not all slaves are mappers\n2.All slaves are mappers\n");
        // printf("Select mode: ");
        // scanf("%d", &mode);
        bool flg2 = true;
        while(flg2 == true)
        {
            srand(time(0));
            // while (mode < 1 || mode > 2)
            // {
            //     printf("Select mode(1/2): ");
            //     scanf("%d", &mode);
            // }

            //if (mode == 1)
            {
                int temp;
                temp = row * row * row *5 *2;
                int div = 0;
                int minReducerNum = 1;
                
                while (temp/minReducerNum >= 20480)
                {
                    minReducerNum++;
                }

                int minMapperNum = worldSize - 1 - minReducerNum;

                if (minReducerNum >= minMapperNum)
                {
                    printf("There are not enough processors as reducer number will have to exceed the number of processors\n");
                    return 0;
                }

                //mapperNum = worldSize - 1 - reducerNum;

                while (reducerNum < minReducerNum || mapperNum <= reducerNum || (mapperNum + reducerNum + 1) != worldSize)
                {
                    mapperNum = (rand() % (worldSize - 1)) + 0;
                    reducerNum = (rand() % (worldSize - 1)) + 0;
                    //printf("kk\n");
                    // if (mapperNum >= (row/2)-1)
                    //     reducerNum = 0;
                }
            }
            // else
            // {
            //     int temp;
            //     temp = row * row * row *5 *2;
            //     int div = 0;
            //     reducerNum = 1;
            //     while (temp/reducerNum > 20000)
            //     {
            //         reducerNum++;
            //     }
            //     mapperNum = worldSize - 1;
            //     // while (reducerNum == 0 || mapperNum <= reducerNum)
            //     // {
            //     //     mapperNum = worldSize - 1;
            //     //     reducerNum = (rand() % ((worldSize/2) - 1)) + 1;
            //     // }
            // }
            flg2 = false;
            if (row/mapperNum == 0)
            {
                // flg2 = true;
                // if (mode == 2){
                printf("The number of mappers allocated too much for the data.\n");
                // printf("Select mode(1/2): ");
                // scanf("%d", &mode);
                // }
                return 0;
            }
                
        }
        printf("Num of Mappers: %d\nNum of Reducers: %d\n\n", mapperNum, reducerNum);

    }


    //Sync
    MPI_Barrier(MPI_COMM_WORLD);


    //master assigning map task to mappers
    int *mapperIds = (int *) malloc(mapperNum * sizeof(int));
    int *reducerIds = (int *) malloc(reducerNum * sizeof(int));
    if (currRank == 0)
    {
        //if (mode == 1)
        {
            for (int i = 1; i <= mapperNum; i++)
                mapperIds[i-1] = i;
            int cnt = 0;
            for (int i = (mapperNum+1); i < worldSize; i++)
            {
                reducerIds[cnt] = i;
                cnt++;
            }
                
        }
        // else
        // {
        //     for (int i = 1; i < worldSize; i++)
        //         mapperIds[i-1] = i;
            
        //     for (int i = 0; i< reducerNum; i++)
        //         reducerIds[i] == 0;
        //     for (int i = 0; i< reducerNum; i++)
        //     {
        //         int flg = 1;
        //         while (flg == 1)
        //         {
        //             reducerIds[i] = (rand() % (worldSize - 1)) + 1;
        //             flg = 0;
        //             for (int j = 0; j< reducerNum; j++)
        //             {
        //                 if (reducerIds[i] == reducerIds[j] && i != j)
        //                 {
        //                     flg = 1;
        //                     break;
        //                 }
        //             }
        //         }
        //     }   
        // }

        // for (int i = 0; i< reducerNum; i++)
        //         printf("%d||", reducerIds[i]);
        // printf("\n");


        int i, j, k;

        //for matrix1
        k = row;
        j = row;
        
        int eachMapperRows = row/mapperNum;
        int count = 0;
        for (int l = 0; l < mapperNum; l++)
        {
            if (mapperIds[l] == mapperIds[mapperNum-1]) //if last mapper
            {
                eachMapperRows = eachMapperRows + (row - (eachMapperRows * mapperNum));
            }
            //making an array to send
            //int *sendArr = (int *) malloc(eachMapperRows * j * sizeof(int));
            int *sendArr = (int*) malloc(row * col * sizeof(int));
            int *sendArr2 = (int*) malloc(row * col * sizeof(int));
            for (int m =0; m< row; m++)
            {
                for (int n = 0; n < col; n++)
                {
                    sendArr[m * row + n] = array1[m][n];
                    sendArr2[m * row + n] = array2[m][n];
                }
                    
            
            }

            //printf("%d\n", eachMapperRows);
            
            // for (int m =0; m< row; m++)
            // {
            //     for (int n = 0; n < col; n++)
            //     {
            //         if (m >= count && m < (count + eachMapperRows))
            //             sendArr[(m-count) * j + n] = array1[m][n];
            //     }
            
            // }

            // printf("\n%d----%d MAP %d ARRAY:\n", count, (count + eachMapperRows-1), mapperIds[l]);
            // for (int m =0; m< eachMapperRows; m++)
            // {
            //     for (int n = 0; n < col; n++)
            //     {
            //         if (n != col - 1)
            //             printf("%d," ,sendArr[m * col + n]);
            //         else
            //             printf("%d" ,sendArr[m * col + n]);
            //     }

            //     printf("\n");
            // }

            int iStart = count;
            int iFinish = count + eachMapperRows-1;
            int indexes[4] = {iStart, iFinish, j, k};
            MPI_Send(indexes, 4, MPI_INT, mapperIds[l], 0, MPI_COMM_WORLD);
            // MPI_Send(sendArr, eachMapperRows * j, MPI_INT, mapperIds[l], 0, MPI_COMM_WORLD);
            MPI_Send(sendArr, row * col, MPI_INT, mapperIds[l], 0, MPI_COMM_WORLD);
            MPI_Send(sendArr2, row * col, MPI_INT, mapperIds[l], 1, MPI_COMM_WORLD);

            printf("Task Map assigned to process %d\n", mapperIds[l]);

            count += eachMapperRows;
                
            

            
        }

    }

    //Sync
    MPI_Barrier(MPI_COMM_WORLD);

    //Broadcasting
    MPI_Bcast(&mapperNum, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(mapperIds, mapperNum, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&reducerNum, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(reducerIds, reducerNum, MPI_INT, 0, MPI_COMM_WORLD);

    MPI_Bcast(&row, 1, MPI_INT, 0, MPI_COMM_WORLD);
    

    //Map tasks
    if (currRank != 0)
    {
        if (isMapper(currRank, mapperIds, mapperNum) == true)
        {
            MPI_Get_processor_name(machineName, &len);
            printf("Process %d recieved task Map on %s\n", currRank, machineName);

            int indexes[4] = {0,0,0,0};
            MPI_Recv(indexes, 4, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            int iStart = indexes[0];
            int iFinish = indexes[1];
            int j = indexes[2];
            int k = indexes[3];


            // int *arr = (int *) malloc((iFinish-iStart+1) * j * sizeof(int));
            int *arr = (int *) malloc(j * j * sizeof(int));
            int *arr2 = (int *) malloc(j * j * sizeof(int));

            //MPI_Recv(arr, (iFinish-iStart+1) * j, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(arr, j * j, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(arr2, j * j, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            

            // printf("\n%d,%d,%d,%d,%d MAP %d ARRAY2:\n", iStart, iFinish, j, k, (iFinish-iStart+1), currRank);
            // for (int m =iStart; m<= iFinish; m++)
            // {
            //     for (int n = 0; n < j; n++)
            //     {
            //         if (n != j - 1)
            //             printf("%d," ,arr2[m * j + n]);
            //         else
            //             printf("%d" ,arr2[m * j + n]);
            //     }

            //     printf("\n");
            // }


            // if (currRank == 2){
            // for (int l = 0; l < k; l++)
            // {
            //     printf("k=%d\n", l);
            //     for (int m = iStart; m <= iFinish; m++)
            //     {
            //         printf("-----i=%d\n", m);
            //         for (int n = 0; n < j; n++)
            //         {
            //             printf("----------j=%d      ((%d,%d), (A, %d, %d)\n", n,m,l,n,arr[(m-iStart)*j+n]);
            //         }
            //     }
            // }}

            //determining the key value pairs
            
            //for matrix 1
            int *keyValPairs1 = (int*) malloc((k * (iFinish-iStart+1) * j) * 5 * sizeof(int));
            int count = 0;
            for (int l = 0; l < k; l++)
            {
                //printf("k=%d\n", l);
                for (int m = iStart; m <= iFinish; m++)
                {
                    //printf("-----i=%d\n", m);
                    for (int n = 0; n < j; n++)
                    {
                        //printf("----------j=%d      ((%d,%d), (0, %d, %d)\n", n,m,l,n,arr[m * j + n]);
                        keyValPairs1[count * 5 + 0] = m;
                        keyValPairs1[count * 5 + 1] = l;
                        keyValPairs1[count * 5 + 2] = 0;
                        keyValPairs1[count * 5 + 3] = n;
                        keyValPairs1[count * 5 + 4] = arr[m * j + n];
                        //printf("---------jj=%d      ((%d,%d), (%d, %d, %d)\n", n,keyValPairs1[count * 5 + 0],keyValPairs1[count * 5 + 1], keyValPairs1[count * 5 + 2],keyValPairs1[count * 5 + 3],keyValPairs1[count * 5 + 4]);
                        count++;
                    }
                }
            }

            // printf("\n\n");
            // for (int m =0; m< (k * (iFinish-iStart+1) * j); m++)
            // {
            //     for (int n = 0; n < 5; n++)
            //     {
            //         if (n != 5 - 1)
            //             printf("%d," ,keyValPairs1[m * 5 + n]);
            //         else
            //             printf("%d" ,keyValPairs1[m * 5 + n]);
            //     }

            //     printf("\n");
            // }

            //for matrix 2
            int *keyValPairs2 = (int*) malloc((k * (iFinish-iStart+1) * j) * 5 * sizeof(int));
            count = 0;
            for (int l = iStart; l <= iFinish; l++)
            {
                //printf("i=%d\n", l);
                for (int m = 0; m < j; m++)
                {
                    //printf("-----j=%d\n", m);
                    for (int n = 0; n < k; n++)
                    {
                        //printf("----------k=%d      ((%d,%d), (0, %d, %d)\n", n,m,l,n,arr[m * j + n]);
                        keyValPairs2[count * 5 + 0] = l;
                        keyValPairs2[count * 5 + 1] = n;
                        keyValPairs2[count * 5 + 2] = 1;
                        keyValPairs2[count * 5 + 3] = m;
                        keyValPairs2[count * 5 + 4] = arr2[m * j + n];
                        //printf("---------kk=%d      ((%d,%d), (%d, %d, %d)\n", n,keyValPairs2[count * 5 + 0],keyValPairs2[count * 5 + 1], keyValPairs2[count * 5 + 2],keyValPairs2[count * 5 + 3],keyValPairs2[count * 5 + 4]);
                        count++;
                    }
                }
            }
            
            MPI_Send(keyValPairs1, (k * (iFinish-iStart+1) * j) * 5, MPI_INT, 0, 0, MPI_COMM_WORLD);
            MPI_Send(keyValPairs2, (k * (iFinish-iStart+1) * j) * 5, MPI_INT, 0, 1, MPI_COMM_WORLD);

        }
        //MPI_Barrier(MPI_COMM_WORLD);
            
    }
    MPI_Barrier(MPI_COMM_WORLD);

    int numKeys;
    //Recieving mapper output and assigning reduce tasks
    if (currRank == 0)
    {
        int *mappersOutput = (int *) malloc(((row * row *row)*2) * 5 * sizeof(int));

        int i, j, k;

        //for matrix1
        k = row;
        j = row;

        int eachMapperRows = row/mapperNum;
        int count = 0;
        int count2 = 0;
        for (int l = 0; l < mapperNum; l++)
        {
            if (mapperIds[l] == mapperIds[mapperNum-1]) //if last mapper
            {
                eachMapperRows = eachMapperRows + (row - (eachMapperRows * mapperNum));
            }

            int iStart = count;
            int iFinish = count + eachMapperRows-1;

            int *recArr = (int*) malloc((k * (iFinish-iStart+1) * j) * 5 * sizeof(int));
            int *recArr2 = (int*) malloc((k * (iFinish-iStart+1) * j) * 5 * sizeof(int));

            MPI_Recv(recArr, (k * (iFinish-iStart+1) * j) * 5, MPI_INT, mapperIds[l], 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(recArr2, (k * (iFinish-iStart+1) * j) * 5, MPI_INT, mapperIds[l], 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);            

            printf("Process %d has completed task Map\n", mapperIds[l]);

            // if (mapperIds[l] == 2)
            // {
            //     printf("\n\n");
            //     for (int m =0; m< (k * (iFinish-iStart+1) * j); m++)
            //     {
            //         for (int n = 0; n < 5; n++)
            //         {
            //             if (n != 5 - 1)
            //                 printf("%d," ,recArr[m * 5 + n]);
            //             else
            //                 printf("%d" ,recArr[m * 5 + n]);
            //         }

            //         printf("\n");
            //     }
            // }

            //printf("\n\n");
            //if (mapperIds[l] == 2)
            {
            for (int m = count2; m < count2 + (k * (iFinish-iStart+1) * j); m++)
            {
                for (int n = 0; n < 5; n++)
                {
                    mappersOutput[m * 5 + n] = recArr[(m-count2)*5+n];
                    //if (mapperIds[l] == 2)
                    // {
                    //     if (n != 5 - 1)
                    //         printf("%d," ,recArr[(m-count2)*5+n]);
                    //     else
                    //         printf("%d" ,recArr[(m-count2)*5+n]);
                    // }
                }
                //printf("\n");
            }
            }

            count2 += k * (iFinish-iStart+1) * j;
            count += eachMapperRows;



            //printf("\n\n");
            //if (mapperIds[l] == 2)
            {
            for (int m = count2; m < count2 + (k * (iFinish-iStart+1) * j); m++)
            {
                for (int n = 0; n < 5; n++)
                {
                    mappersOutput[m * 5 + n] = recArr2[(m-count2)*5+n];
                    //if (mapperIds[l] == 2)
                    // {
                    //     if (n != 5 - 1)
                    //         printf("%d," ,recArr[(m-count2)*5+n]);
                    //     else
                    //         printf("%d" ,recArr[(m-count2)*5+n]);
                    // }
                }
                //printf("\n");
            }}

            count2 += k * (iFinish-iStart+1) * j;
        }

        // for (int m =0; m< 2*row*row*row; m++)
        // {
        //     for (int n = 0; n < 5; n++)
        //     {
        //         if (n != 5 - 1)
        //             printf("%d," ,mappersOutput[m * 5 + n]);
        //         else
        //             printf("%d" ,mappersOutput[m * 5 + n]);
        //     }

        //     printf("\n");
        // }


        //Sort the mapper output

        numKeys = row*row;
        int uniqueKeys[numKeys][2];
        int *sortedMappersOutput = (int *) malloc(((row * row *row)*2) * 5 * sizeof(int));

        int index = 0;
        for (int m = 0; m < row; m++)
        {
            for (int n = 0; n< row; n++)
            {
                uniqueKeys[index][0] = m;
                uniqueKeys[index][1] = n;
                index++;
            }
        }


        index = 0;
        for (int m = 0; m < numKeys; m++)
        {
            for (int n =0; n < (row * row *row)*2; n++)
            {
                if (uniqueKeys[m][0] == mappersOutput[n*5+0] && uniqueKeys[m][1] == mappersOutput[n*5+1])
                {
                    for (int p = 0; p < 5; p++)
                    {
                        sortedMappersOutput[index * 5 + p] = mappersOutput[n * 5 + p];
                    }
                    index++;
                }
            }
        }

        // printf("\n\n");
        // for (int m =0; m< 2*row*row*row; m++)
        // {
        //     for (int n = 0; n < 5; n++)
        //     {
        //         if (n != 5 - 1)
        //             printf("%d," ,sortedMappersOutput[m * 5 + n]);
        //         else
        //             printf("%d" ,sortedMappersOutput[m * 5 + n]);
        //     }

        //     printf("\n");
        // }


        //Dividing the sorted mapper output amongst the reducers
        //Ensure that the same key val is not given to more than 1 reducer
        //each key has row*2 values

        
        int uniqueKeysPerRed = numKeys/reducerNum;

        count = 0;
        int keyStart = 0;
        for (int l = 0; l< reducerNum; l++)
        {
            if (reducerIds[l] == reducerIds[reducerNum-1]) //if last reducer
            {
                uniqueKeysPerRed = uniqueKeysPerRed + (numKeys - (uniqueKeysPerRed * reducerNum));
            }

            int iStart = count;
            int iFinish = count + (row * 2 * uniqueKeysPerRed) - 1;

            int *sendArr = (int *)malloc((row * 2 * uniqueKeysPerRed) * 5 * sizeof(int));

            for (int m = iStart; m < (iFinish+1); m++)
            {
                for (int n =0; n < 5; n++)
                    sendArr[(m-iStart)*5+n] = sortedMappersOutput[m*5+n];
            }

            //if (reducerIds[l] == reducerIds[0])
            // {
            //     //printf("\n\n");
            //     for (int m =0; m< (row * 2 * uniqueKeysPerRed); m++)
            //     {
            //         for (int n = 0; n < 5; n++)
            //         {
            //             if (n != 5 - 1)
            //                 printf("%d," ,sendArr[m * 5 + n]);
            //             else
            //                 printf("%d" ,sendArr[m * 5 + n]);
            //         }

            //         printf("\n");
            //     }
            // }
            
            int indexes[4] = {iStart, iFinish, uniqueKeysPerRed, keyStart};
            
            MPI_Send(indexes, 4, MPI_INT, reducerIds[l], 0, MPI_COMM_WORLD);
            //printf("hehe\n");
            //MPI_Send(sortedMappersOutput, ((row * row *row)*2) * 5, MPI_INT, reducerIds[l], 0, MPI_COMM_WORLD);
            MPI_Send(sendArr, (row * 2 * uniqueKeysPerRed) * 5, MPI_INT, reducerIds[l], 0, MPI_COMM_WORLD);
            //printf("hehe1\n");
            MPI_Send(uniqueKeys, numKeys*2, MPI_INT, reducerIds[l], 1, MPI_COMM_WORLD);
            //printf("hehe2\n");
            printf("Task Reduce assigned to process %d\n", reducerIds[l]);

            count += row * 2 * uniqueKeysPerRed;
            keyStart += uniqueKeysPerRed;

        }

    }

    MPI_Barrier(MPI_COMM_WORLD);

    //Reduce tasks
    if (currRank != 0)
    {
        if (isReducer(currRank, reducerIds, reducerNum) == true)
        {
            MPI_Get_processor_name(machineName, &len);
            printf("Process %d recieved task Reduce on %s\n", currRank, machineName);

            int indexes[4] = {0,0,0,0};
            MPI_Recv(indexes, 4, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            int iStart = indexes[0];
            int iFinish = indexes[1];
            int uniqueKeysPerRed = indexes[2];
            int keyStart = indexes[3];

            //int *arr = (int *) malloc(((row * row *row)*2) * 5 * sizeof(int));
            int *arr = (int *) malloc((row * 2 * uniqueKeysPerRed) * 5 * sizeof(int));
            int uniqueKeys[row*row][2];

            //MPI_Recv(arr, ((row * row *row)*2) * 5, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(arr, (row * 2 * uniqueKeysPerRed) * 5, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            MPI_Recv(uniqueKeys, row*row*2, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);          

            // printf("\n%d,%d,%d REDUCE %d ARRAY:\n", iStart, iFinish, (iFinish-iStart+1), currRank);
            // for (int m =0; m< (row * 2 * uniqueKeysPerRed); m++)
            // {
            //     for (int n = 0; n < 5; n++)
            //     {
            //         if (n != 5 - 1)
            //             printf("%d," ,arr[m * 5 + n]);
            //         else
            //             printf("%d" ,arr[m * 5 + n]);
            //     }

            //     printf("\n");
            // }

            int count3 = 0;
            int count4 = 0;
            int mulArray[row];
            //int keyVal[uniqueKeysPerRed][3];
            int *keyVal = (int *) malloc(uniqueKeysPerRed * 3 * sizeof(int));
            int count5 = keyStart;
            for (int m =0; m< (row * 2 * uniqueKeysPerRed); m++)
            {
                if (arr[m * 5 + 2] == 0)
                {
                    for (int n = m+1; n<= iFinish; n++)
                    {
                        if (arr[n*5+2] == 1 && arr[m*5+3] == arr[n*5+3])
                        {
                            mulArray[count3] = arr[m*5+4] * arr[n*5+4];
                            break;
                        }
                    }
                    count3++;
                    if (count3 == row)
                    {
                        keyVal[count4 * 3 + 2] = 0;
                        keyVal[count4 * 3 + 0] = uniqueKeys[count5][0];
                        keyVal[count4 * 3 + 1] = uniqueKeys[count5][1];
                        for (int p = 0; p < row; p++)
                            keyVal[count4 * 3 + 2] += mulArray[p];
                        count4++;
                        count5++;
                        count3 = 0;
                    }
                }
                
            }

            
            // for (int m=0; m < uniqueKeysPerRed; m++)
            //    printf("Reducer: %d key: %d,%d : %d\n",currRank, keyVal[m * 3 + 0], keyVal[m * 3 + 1], keyVal[m * 3 + 2]);
            
            // MPI_File fi;
            // MPI_File_open(MPI_COMM_WORLD, "keyValReducer.txt", MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL, &fi);

            if (currRank == reducerIds[0])
                file = fopen("keyValReducer.txt","w");
            else
                file = fopen("keyValReducer.txt","a");

            for (int m = 0; m<uniqueKeysPerRed; m++)
            {
                fprintf(file,"(%d,%d)->%d",keyVal[m*3+0], keyVal[m*3+1], keyVal[m*3+2]);
                fprintf(file,"\n");
            }
            // printf("hehe\n");
            // MPI_File_write_ordered(fi, keyVal, uniqueKeysPerRed*3, MPI_INT, MPI_STATUS_IGNORE);

            MPI_Send(keyVal, uniqueKeysPerRed * 3, MPI_INT, 0, 0, MPI_COMM_WORLD);


        }
    }

    MPI_Barrier(MPI_COMM_WORLD);

    //Recieving reducer output and checking output
    if (currRank == 0)
    {
        int *reducerOutput = (int *) malloc((row * row) * 3 * sizeof(int));

        int uniqueKeysPerRed = numKeys/reducerNum;

        int count = 0;
        int count2 = 0;
        int keyStart = 0;
        for (int l = 0; l< reducerNum; l++)
        {
            if (reducerIds[l] == reducerIds[reducerNum-1]) //if last reducer
            {
                uniqueKeysPerRed = uniqueKeysPerRed + (numKeys - (uniqueKeysPerRed * reducerNum));
            }

            int iStart = count;
            int iFinish = count + (row * 2 * uniqueKeysPerRed) - 1;

            int *recArr = (int*) malloc((uniqueKeysPerRed) * 3 * sizeof(int));

            MPI_Recv(recArr, (uniqueKeysPerRed) * 3, MPI_INT, reducerIds[l], 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            printf("Process %d has completed task Reduce\n", reducerIds[l]);

            // for (int m =0; m< uniqueKeysPerRed; m++)
            // {
            //     for (int n = 0; n < 3; n++)
            //     {
            //         if (n != 3 - 1)
            //             printf("%d," ,recArr[m * 3 + n]);
            //         else
            //             printf("%d" ,recArr[m * 3 + n]);
            //     }

            //     printf("\n");
            // }
            // printf("\n");

            //Combining the reducer output
            for (int m = count2; m < count2 + (uniqueKeysPerRed); m++)
            {
                for (int n = 0; n < 3; n++)
                {
                    reducerOutput[m * 3 + n] = recArr[(m-count2)*3+n];
                }
            }

            count += row * 2 * uniqueKeysPerRed;
            count2 += uniqueKeysPerRed;

        }

        printf("\nJob has been completed!!!\n");

        // for (int m =0; m< row*row; m++)
        // {
        //     for (int n = 0; n < 3; n++)
        //     {
        //         if (n != 3 - 1)
        //             printf("%d," ,reducerOutput[m * 3 + n]);
        //         else
        //             printf("%d" ,reducerOutput[m * 3 + n]);
        //     }

        //     printf("\n");
        // }

        //Converting the key value pairs to a matrix
        int *resultantMat = (int *) malloc(row*row*sizeof(int));

        int valCount = 0;
        for (int m =0; m< row; m++)
        {
            for (int n = 0; n < row; n++)
            {
                resultantMat[m*row+n] = reducerOutput[valCount*3+2];
                valCount++;
            }
        }


        // for (int m =0; m< row; m++)
        // {
        //     for (int n = 0; n < row; n++)
        //     {
        //         if (n != row - 1)
        //             printf("%d," ,resultantMat[m*row+n]);
        //         else
        //             printf("%d" ,resultantMat[m*row+n]);
        //     }

        //     printf("\n");
        // }

        //Writing the matrix to file
        file = fopen("result.txt","w");
        for (int m =0; m < row; m++)
        {
            for (int n =0; n < row; n++)
            {
                if (n != row - 1)
                    fprintf(file,"%d,",resultantMat[m*row+n]);
                else
                    fprintf(file,"%d",resultantMat[m*row+n]);
            }
            fprintf(file,"\n");
        }


        //Matrix comparison

        //Calculating the serial result
        int *serialResultantMat = (int *) malloc(row*row*sizeof(int));

        for (int m = 0; m < row; m++)
        {
            for (int n = 0; n < row; n++)
            {
                serialResultantMat[m*row+n] = 0;
                for (int p = 0; p < row; p++)
                {
                    serialResultantMat[m*row+n] += array1[m][p] * array2[p][n];
                }
            }
        }

        if (matrixComparison(resultantMat, serialResultantMat, row) == true)
            printf("Matrix comparison function returned: TRUE :)\n");
        else
            printf("Matrix comparison function returned: FALSE :(\n");


    }

    

    // Finalize the MPI environment.
	MPI_Finalize();
}