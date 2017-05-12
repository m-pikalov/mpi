#include "mpi.h"
#include "stdlib.h"
#include "time.h"
#include "stdio.h"
#include "math.h"

int main(int argc, char *argv[]){

   int size = 2500;
   srand(time(NULL));

   int procRank = 0, procNum = 0, nameLen = 0, i = 0, j = 0, k = 0;
   char procName[MPI_MAX_PROCESSOR_NAME];
   double startTime = 0, endTime = 0;

   double* matrix;
   double* vector;

   MPI_Init(&argc, &argv);
   MPI_Comm_size(MPI_COMM_WORLD, &procNum);
   MPI_Comm_rank(MPI_COMM_WORLD, &procRank);
   MPI_Get_processor_name(procName, &nameLen);
   MPI_Status Status;

   fprintf(stderr, "Process %d on %s\n", procRank, procName);

   if(procRank == 0){
      matrix = (double*)malloc(size * size * sizeof(double));
      vector = (double*)malloc(size * sizeof(double));

      for(i = 0; i < size; ++i){
         for(j = 0; j < size; ++j){
            matrix[i * size + j] = rand() % 100000;
         }

         vector[i] = rand() % 100000;
      }

      /*for(i = 0; i < size; ++i){
         for(j = 0; j < size; ++j){
            printf("%.9f ", matrix[i * size + j]);
         }
         printf("\n");
      }
      printf("\n");

      for(i = 0; i < size; ++i){
         printf("%.9f ", vector[i]);
      }
      printf("\n\n");*/
   }

   startTime = MPI_Wtime();

   if((size < procNum) && (procRank == 0)){

      //TODO write sequential algorithm
      free(matrix);

   } else if(size >= procNum){

        double* pivotRow;
        int* sendCount = (int*)malloc(procNum * sizeof(int));
        int* sendInd = (int*)malloc(size * sizeof(int));
        int* localInd;
        int* localTransposInd;

        for(i = 0; i < procNum; ++i){
           sendCount[i] = 0;
        }

        for(i = 0; i < size; ++i){
           ++(sendCount[i % procNum]);
           sendInd[i] = i % procNum;
        }

        int numberOfRows = sendCount[procRank];

        free(sendCount);

        localInd = (int*)malloc(numberOfRows * sizeof(int));
        localTransposInd = (int*)malloc(numberOfRows * sizeof(int));

        int ind = 0;
        for(i = 0; i < size; ++i){
           if(sendInd[i] == procRank){
              localInd[ind] = i;
              localTransposInd[ind] = i;
              ++ind;
           }
        }

        double* subMatrix = (double*)malloc(numberOfRows * size * sizeof(double));

        if(procRank == 0){
           for(i = 0; i < numberOfRows; ++i){
              for(j = 0; j < size; ++j){
                 subMatrix[i * size + j] = matrix[localInd[i] * size + j];
              }
           }
           for(i = 0; i < size; ++i){
              if(sendInd[i] != 0){
                 MPI_Send(matrix + i * size, size, MPI_DOUBLE, sendInd[i], i, MPI_COMM_WORLD);
              }
           }
        } else {
             for(i = 0; i < numberOfRows; ++i){
                MPI_Recv(subMatrix + i * size, size, MPI_DOUBLE, 0, localInd[i], MPI_COMM_WORLD, &Status);
             }
        }

        if(procRank == 0){
           free(matrix);
        }

        for(i = 0; i < size; ++i){
           int pivotRank = 0;
           double pivotVal = 0;
           int pivotInd = -1;
           double pivot[3];
           double bufPivot[2];
           if(procRank == 0){
              for(j = 0; j < numberOfRows; ++j){
                 if((localInd[j] >= i) && (fabs(subMatrix[j * size + i]) > pivotVal)){
                    pivotVal = fabs(subMatrix[j * size + i]);
                    pivotInd = localInd[j];
                 }
              }

              for(j = 1; j < procNum; ++j){
                 MPI_Recv(&bufPivot, 2, MPI_DOUBLE, j, i, MPI_COMM_WORLD, &Status);
                 if((int)(round(bufPivot[1]) >= 0) && (bufPivot[0] > pivotVal)){
                    pivotRank = j;
                    pivotVal = bufPivot[0];
                    pivotInd = round(bufPivot[1]);
                 }
              }

              pivot[0] = pivotVal;
              pivot[1] = (double)pivotInd;
              pivot[2] = (double)pivotRank;
           } else {
                for(j = 0; j < numberOfRows; ++j){
                   if((localInd[j] >= i ) && (fabs(subMatrix[j * size + i]) > pivotVal)){
                      pivotVal = fabs(subMatrix[j * size + i]);
                      pivotInd = localInd[j];
                   }
                }

                bufPivot[0] = pivotVal;
                bufPivot[1] = (double)pivotInd;

                MPI_Send(&bufPivot, 2, MPI_DOUBLE, 0, i, MPI_COMM_WORLD);
           }

           MPI_Bcast(&pivot, 3, MPI_DOUBLE, 0, MPI_COMM_WORLD);

           if((int)round(pivot[2]) != sendInd[i]){
              if(procRank == (int)round(pivot[2])){
                 for(j = 0; j < numberOfRows; ++j){
                    if(localInd[j] == (int)round(pivot[1])){
                       localInd[j] = i;
                       break;
                    }
                 }
              }

              if(procRank == sendInd[i]){
                 for(j = 0; j < numberOfRows; ++j){
                    if(localInd[j] == i){
                       localInd[j] = (int)round(pivot[1]);
                       break;
                    }
                 }
              }
           } else {
                if(procRank == sendInd[i]){
                   int bufIndexOne = 0;
                   int bufIndexTwo = 0;
                   for(j = 0; j < numberOfRows; ++j){
                      if(localInd[j] == i){
                         bufIndexOne = j;
                      }
                      if(localInd[j] == (int)round(pivot[1])){
                           bufIndexTwo = j;
                      }
                   }
                   localInd[bufIndexOne] = (int)round(pivot[1]);
                   localInd[bufIndexTwo] = i;
                }
           }

           sendInd[(int)round(pivot[1])] = sendInd[i];
           sendInd[i] = (int)round(pivot[2]);

           pivotRow = (double*)malloc(size * sizeof(double));

           if(procRank == (int)round(pivot[2])){
              for(j = 0; j < numberOfRows; ++j){
                 if(localInd[j] == i){
                    for(k = 0; k < size; ++k){
                       pivotRow[k] = subMatrix[j * size + k];
                    }
                    break;
                 }
              }
           }

           MPI_Bcast(pivotRow, size, MPI_DOUBLE, (int)round(pivot[2]), MPI_COMM_WORLD);

           for(j = 0; j < numberOfRows; ++j){
              if(localInd[j] > i){
                 subMatrix[j * size + i] =  subMatrix[j * size + i] / pivotRow[i];
                 for(k = i + 1; k < size; ++k){
                    subMatrix[j * size + k] -= subMatrix[j * size + i] * pivotRow[k];
                 }
              }
           }

           MPI_Barrier(MPI_COMM_WORLD);

           free(pivotRow);
        }

        if(procRank != 0){
           vector = (double*)malloc(size * sizeof(double));
        }

        MPI_Bcast(vector, size, MPI_DOUBLE, 0, MPI_COMM_WORLD);

        double* yVector = (double*)malloc(size * sizeof(double));

        for(i = 0; i < size; ++i){
           if(procRank == sendInd[i]){
              for(j = 0; j < numberOfRows; ++j){
                 if(localInd[j] == i){
                    double buf = vector[localTransposInd[j]];
                    for(k = 0; k < i; ++k){
                       buf -= subMatrix[j * size + k] * yVector[k];
                    }
                    yVector[i] = buf;
                    if((i < size - 1) && (sendInd[i + 1] != sendInd[i])){
                       MPI_Ssend(yVector, i + 1, MPI_DOUBLE, sendInd[i + 1], 0, MPI_COMM_WORLD);
                    }
                    break;
                 }
              }
           }
           if((procRank == sendInd[i + 1]) && (sendInd[i + 1] != sendInd[i]) && (i < size - 1)){
                MPI_Recv(yVector, i + 1, MPI_DOUBLE, sendInd[i], 0, MPI_COMM_WORLD, &Status);
           }
           MPI_Barrier(MPI_COMM_WORLD);
        }

        free(vector);

        MPI_Bcast(yVector, size, MPI_DOUBLE, sendInd[size - 1], MPI_COMM_WORLD);

        double* xVector = (double*)malloc(size * sizeof(double));

        for(i = size - 1; i >= 0; --i){
           if(procRank == sendInd[i]){
              for(j = 0; j < numberOfRows; ++j){
                 if(localInd[j] == i){
                    double buf = yVector[i];
                    for(k = i + 1; k < size; ++k){
                       buf -= subMatrix[j * size + k] * xVector[k];
                    }
                    buf /= subMatrix[j * size + i];
                    xVector[i] = buf;
                    if((i > 0) && (sendInd[i - 1] != sendInd[i])){
                       MPI_Ssend(xVector + i, size - i, MPI_DOUBLE, sendInd[i - 1], 0, MPI_COMM_WORLD);
                    }
                 }
              }
           }
           if ((procRank == sendInd[i - 1]) && (sendInd[i - 1] != sendInd[i]) && (i > 0)){
                MPI_Recv(xVector + i, size - i, MPI_DOUBLE, sendInd[i], 0, MPI_COMM_WORLD, &Status);
           }
           MPI_Barrier(MPI_COMM_WORLD);
        }

        if((procRank == sendInd[0]) && (sendInd[0] != 0)){
           MPI_Ssend(xVector, size, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
        }

        if((procRank == 0) && (sendInd[0] != 0)){
           MPI_Recv(xVector, size, MPI_DOUBLE, sendInd[0], 0, MPI_COMM_WORLD, &Status);
        }

        if(procRank == 0){
           vector = xVector;
        }

        free(subMatrix);
        free(sendInd);
        free(localInd);
        free(localTransposInd);
        free(yVector);
        if(procRank != 0){
           free(xVector);
        }
   }

   endTime = MPI_Wtime();

   if(procRank == 0){

      /*for(i = 0; i < size; ++i){
         printf("%.9f ", vector[i]);
      }
      printf("\n");*/

      printf("\nTime = %f\n", (endTime - startTime));

      free(vector);
   }

   MPI_Finalize();
   return 0;
}