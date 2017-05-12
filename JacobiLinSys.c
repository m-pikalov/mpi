#include "mpi.h"
#include "stdlib.h"
#include "time.h"
#include "stdio.h"
#include "limits.h"
#include "math.h"

double DotProduct(double* vectorOne, double* vectorTwo, int size){

   int i = 0;
   double result = 0;

   for(i = 0; i < size; ++i){
      result += vectorOne[i] * vectorTwo[i];
   }

   return result;
}

void VecSum(double* vectorOne, double* vectorTwo, int size){

   int i = 0;

   for(i = 0; i < size; ++i){
      vectorOne[i] += vectorTwo[i];
   }
}

int main(int argc, char *argv[]){

   int size = 10000;
   double max = INT_MAX;
   srand(time(NULL));

   int procRank = 0, procNum = 0, nameLen = 0, i = 0, j = 0;
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
         double bufValue = 0;

         for(j = 0; j < size; ++j){
            if(i != j){
               matrix[i * size + j] = rand() % 1000;
            }
            bufValue += matrix[i * size + j];
         }

          matrix[i * size + i] = bufValue + rand() % (int)(INT_MAX - bufValue);
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

      double* newVector = (double*)malloc(size * sizeof(double));

      for(i = 0; i < size; ++i){
         newVector[i] = vector[i] / matrix[i * size + i];
      }

      free(vector);

      double* newMatrix = (double*)malloc(size * size * sizeof(double));

      for(i = 0; i < size; ++i){
         for(j = 0; j < size; ++j){
            newMatrix[i * size + j] = i == j? 0 : -matrix[i * size + j]/matrix[i * size + i];
         }
      }

      free(matrix);

      double* sumVector = (double*)malloc(size * sizeof(double));

      for(i = 0; i < size; ++i){
         sumVector[i] = newVector[i];
      }

      do{

         vector = (double*)malloc(size * sizeof(double));

         for(i = 0; i < size; ++i){
            vector[i] = DotProduct(newMatrix + i * size, newVector, size);
         }

         VecSum(vector, sumVector, size);

         max = INT_MIN;

         for(i = 0; i < size; ++i){
            if(fabs(vector[i] - newVector[i]) > max){
               max = fabs(vector[i] - newVector[i]);
            }
         }

         free(newVector);
         newVector = vector;

      } while(max > 0.0000000001);

      free(newMatrix);
      free(sumVector);

   } else if(size >= procNum){

        int* sendCount = (int*)malloc(procNum * sizeof(int));
        int* sendPoint = (int*)malloc(procNum * sizeof(int));
        double* newVector;
        double* newSubMatrix;

        for(i = 0; i < procNum; ++i){
           sendCount[i] = 0;
        }

        for(i = 0; i < size; ++i){
           ++sendCount[i % procNum];
        }

        sendPoint[0] = 0;
        for(i = 1; i < procNum; ++i){
           sendPoint[i] = sendPoint[i - 1] + sendCount[i - 1];
        }

        for(i = 0; i < procNum; ++i){
           sendCount[i] *= size;
           sendPoint[i] *= size;
        }

        int numberOfRows = (int)(sendCount[procRank] / size);
        int displace = (int)(sendPoint[procRank] / size);

        newVector = (double*)malloc(size * sizeof(double));

        if(procRank == 0){
           for(i = 0; i < size; ++i){
              newVector[i] = vector[i] / matrix[i * size + i];
           }
        }

        MPI_Bcast(newVector, size, MPI_DOUBLE, 0, MPI_COMM_WORLD);

        if(procRank == 0){
           free(vector);
        }

        double* newMatrix = (double*)malloc(size * numberOfRows * sizeof(double));

        MPI_Scatterv(matrix, sendCount, sendPoint, MPI_DOUBLE, newMatrix, numberOfRows * size, MPI_DOUBLE, 0, MPI_COMM_WORLD);

        if(procRank == 0){
           free(matrix);
        }

        for(i = 0; i < numberOfRows; ++i){
           for(j = 0; j < size; ++j){
              newMatrix[i * size + j] = displace + i == j? newMatrix[i * size + j] : -newMatrix[i * size + j]/newMatrix[i * size + i + displace];
           }
           newMatrix[i * size + i + displace] = 0;
        }

        for(i = 0; i < procNum; ++i){
           sendCount[i] /= size;
           sendPoint[i] /= size;
        }

        double* vectorToSum = (double*)malloc(numberOfRows * sizeof(double));

        for(i = 0; i < numberOfRows; ++i){
           vectorToSum[i] = newVector[displace + i];
        }

        do{

           double* bufVector = (double*)malloc(numberOfRows * sizeof(double));

           for(i = 0; i < numberOfRows; ++i){
              bufVector[i] = DotProduct(newMatrix + i * size, newVector, size);
           }

           VecSum(bufVector, vectorToSum, numberOfRows);

           vector = (double*)malloc(size * sizeof(double));

           MPI_Allgatherv(bufVector, numberOfRows, MPI_DOUBLE, vector, sendCount, sendPoint, MPI_DOUBLE, MPI_COMM_WORLD);

           free(bufVector);

           max = INT_MIN;

           for(i = 0; i < size; ++i){
              if(fabs(newVector[i] - vector[i]) > max){
                 max = fabs(newVector[i] - vector[i]);
              }
           }

           free(newVector);
           newVector = vector;
        } while (max > 0.000000000001);

        if(procRank != 0){
           free(vector);
        }

        free(vectorToSum);
        free(newMatrix);
        free(sendCount);
        free(sendPoint);
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