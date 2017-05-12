#include "mpi.h"
#include "stdlib.h"
#include "time.h"
#include "stdio.h"
#include "limits.h"

void Divide(int* arrayToDivide, int** dividedArray, int sizeOfArrayToDivide, int** sizeOfSubArrays, int** pointsOfSubArrays, int* mainPivots, int procNum){

   int i = 0, j = 0;

   *dividedArray = (int*)malloc(sizeOfArrayToDivide * sizeof(int));
   *sizeOfSubArrays = (int*)malloc(procNum * sizeof(int));
   *pointsOfSubArrays = (int*)malloc(procNum * sizeof(int));
   int* indOfSubArrays = (int*)malloc(procNum * sizeof(int));

   for(i = 0; i < procNum; ++i){
      (*sizeOfSubArrays)[i] = 0;
   }

   for(i = 0; i < sizeOfArrayToDivide; ++i){
      for(j = 0; j < procNum; ++j){
         if((arrayToDivide[i] <= mainPivots[j + 1]) && (arrayToDivide[i] > mainPivots[j])){
            ++(*sizeOfSubArrays)[j];
         }
      }
   }

   *pointsOfSubArrays[0] = 0;
   indOfSubArrays[0] = 0;
   for(i = 1; i < procNum; ++i){
      (*pointsOfSubArrays)[i] = (*pointsOfSubArrays)[i - 1] + (*sizeOfSubArrays)[i - 1];
      indOfSubArrays[i] = (*pointsOfSubArrays)[i];
   }

   for(i = 0; i < sizeOfArrayToDivide; ++i){
      for(j = 0; j < procNum; ++j){
         if((arrayToDivide[i] <= mainPivots[j + 1]) && (arrayToDivide[i] > mainPivots[j])){
            (*dividedArray)[indOfSubArrays[j]] = arrayToDivide[i];
            ++indOfSubArrays[j];
         }
      }
   }

   free(indOfSubArrays);
}

int* Merge(int* arrayToMerge, int* sizeOfSubArrays, int sizeOfArrayToMerge, int procNum) {

   int i = 0, j = 0;

   int* mergedArray = (int*)malloc(sizeOfArrayToMerge * sizeof(int));
   int* indOfSubArrays = (int*)malloc(procNum * sizeof(int));
   int* boundsOfSubArrays = (int*)malloc(procNum * sizeof(int));

   indOfSubArrays[0] = 0;
   boundsOfSubArrays[0] = sizeOfSubArrays[0];
   for (i = 1; i < procNum; ++i) {
      indOfSubArrays[i] = indOfSubArrays[i - 1] + sizeOfSubArrays[i - 1];
      boundsOfSubArrays[i] = boundsOfSubArrays[i - 1] + sizeOfSubArrays[i];
   }

   for (i = 0; i < sizeOfArrayToMerge; ++i) {
      int minValue = INT_MAX;
      int indOfProc = 0;

      for (j = 0; j < procNum; ++j) {
         if ((arrayToMerge[indOfSubArrays[j]] < minValue) && (indOfSubArrays[j] < boundsOfSubArrays[j])) {
            minValue = arrayToMerge[indOfSubArrays[j]];
            indOfProc = j;
         }
      }

      ++indOfSubArrays[indOfProc];
      mergedArray[i] = minValue;
   }

   free(indOfSubArrays);
   free(boundsOfSubArrays);

   return  mergedArray;
}

void QuickSort(int* arrayToSort, int leftBound, int rightBound) {

   if (leftBound >= rightBound) {
      return;
   }

   int leftInd = leftBound, rightInd = rightBound, buf = 0;

   int pivotInd = leftBound, pivot = arrayToSort[pivotInd];

   while (1) {

      while ((arrayToSort[leftInd] <= pivot) && (leftInd < rightBound)) {
         ++leftInd;
      }

      while ((arrayToSort[rightInd] >= pivot) && (rightInd > leftBound)) {
         --rightInd;
      }

      if (leftInd >= rightInd) {
         break;
      }

      buf = arrayToSort[leftInd];
      arrayToSort[leftInd] = arrayToSort[rightInd];
      arrayToSort[rightInd] = buf;
    }

    buf = arrayToSort[pivotInd];
    arrayToSort[pivotInd] = arrayToSort[rightInd];
    arrayToSort[rightInd] = buf;

    QuickSort(arrayToSort, leftBound, rightInd - 1);
    QuickSort(arrayToSort, rightInd + 1, rightBound);
}

int main(int argc, char *argv[]){

   int size = 100000000;
   srand(time(NULL));

   int procRank = 0, procNum = 0, nameLen = 0, i = 0, j = 0;
   char procName[MPI_MAX_PROCESSOR_NAME];
   double startTime = 0, endTime = 0;

   int* arrayToSort;
   int* pivots;
   int* allPivots;
   int* mainPivots;

   MPI_Init(&argc, &argv);
   MPI_Comm_size(MPI_COMM_WORLD, &procNum);
   MPI_Comm_rank(MPI_COMM_WORLD, &procRank);
   MPI_Get_processor_name(procName, &nameLen);
   MPI_Status Status;

   fprintf(stderr, "Process %d on %s\n", procRank, procName);

   if(procRank == 0){

      arrayToSort = (int*)malloc(size * sizeof(int));

      for(i = 0; i < size; ++i){
         arrayToSort[i] = rand();
      }

      /*for(i = 0; i < size; ++i){
         printf("%d ", arrayToSort[i]);
      }
      printf("\n\n");*/
   }

   startTime = MPI_Wtime();

   if((size < procNum * procNum) && (procRank == 0)){

      QuickSort(arrayToSort, 0, size - 1);

   } else if (size >= procNum * procNum) {
        int* sendCount = (int*)malloc(procNum * sizeof(int));
        int* sendPoint = (int*)malloc(procNum * sizeof(int));

        for(i = 0; i < procNum; ++i){
           sendCount[i] = size / procNum;
           sendPoint[i] = i * sendCount[i];
        }
        sendCount[procNum - 1] += size % procNum;

        int* subArray = (int*)malloc(sendCount[procRank]* sizeof(int));

        MPI_Scatterv(arrayToSort, sendCount, sendPoint, MPI_INT, subArray, sendCount[procRank], MPI_INT, 0, MPI_COMM_WORLD);

        if(procRank == 0){
           free(arrayToSort);
        }

        int sendLength = sendCount[procRank];

        free(sendCount);
        free(sendPoint);

        QuickSort(subArray, 0, sendLength - 1);

        pivots = (int*)malloc(procNum * sizeof(int));

        for(i = 0; i < procNum; ++i){
           pivots[i] = subArray[(i * size)/(procNum * procNum)];
        }

        if(procRank == 0){
           allPivots = (int*)malloc(procNum * procNum * sizeof(int));
        }

        MPI_Gather(pivots, procNum, MPI_INT, allPivots, procNum, MPI_INT, 0, MPI_COMM_WORLD);

        free(pivots);

        mainPivots = (int*)malloc((procNum + 1) * sizeof(int));

        if(procRank == 0){

           QuickSort(allPivots, 0, procNum * procNum - 1);

           for(i = 1; i < procNum; ++i){
              mainPivots[i] = allPivots[i * procNum + procNum / 2 - 1];
           }
           mainPivots[0] = INT_MIN;
           mainPivots[procNum] = INT_MAX;

           free(allPivots);
        }

        MPI_Bcast(mainPivots, procNum + 1, MPI_INT, 0, MPI_COMM_WORLD);

        int* dividedArray;
        int* sizeOfSubArrays;
        int* pointsOfSubArrays;

        Divide(subArray, &dividedArray, sendLength, &sizeOfSubArrays, &pointsOfSubArrays, mainPivots, procNum);

        free(subArray);

        int* recvSize = (int*)malloc(procNum * sizeof(int));

        MPI_Alltoall(sizeOfSubArrays, 1, MPI_INT, recvSize, 1, MPI_INT, MPI_COMM_WORLD);

        int* recvPoints = (int*)malloc(procNum * sizeof(int));
        int recvLength = recvSize[0];

        recvPoints[0] = 0;
        for(i = 1; i < procNum; ++i){
           recvPoints[i] = recvPoints[i - 1] + recvSize[i - 1];
           recvLength += recvSize[i];
        }

        subArray = (int*)malloc(recvLength * sizeof(int));

        MPI_Alltoallv(dividedArray, sizeOfSubArrays, pointsOfSubArrays, MPI_INT, subArray, recvSize, recvPoints, MPI_INT, MPI_COMM_WORLD);

        free(recvPoints);
        free(dividedArray);
        free(sizeOfSubArrays);
        free(pointsOfSubArrays);

        dividedArray = Merge(subArray, recvSize, recvLength, procNum);

        free(recvSize);
        free(subArray);

        if(procRank == 0){
           sendCount = (int*)malloc(procNum * sizeof(int));
        }

        MPI_Gather(&recvLength, 1, MPI_INT, sendCount, 1, MPI_INT, 0, MPI_COMM_WORLD);

        if(procRank == 0){
           sendPoint = (int*)malloc(procNum * sizeof(int));

           sendPoint[0] = 0;
           for(i = 1; i < procNum; ++i){
              sendPoint[i] = sendPoint[i - 1] + sendCount[i - 1];
           }

           arrayToSort = (int*)malloc(size * sizeof(int));
        }

        MPI_Gatherv(dividedArray, recvLength, MPI_INT, arrayToSort, sendCount, sendPoint, MPI_INT, 0, MPI_COMM_WORLD);

        free(dividedArray);

        if(procRank == 0){
           free(sendPoint);
           free(sendCount);
        }
   }

   endTime = MPI_Wtime();

   if(procRank == 0){
      /*for(i = 0; i < size; ++i){
         printf("%d ", arrayToSort[i]);
      }
      printf("\n");*/

      printf("\nTime = %f\n", (endTime - startTime));
      free(arrayToSort);
   }

   MPI_Finalize();
   return 0;
}
